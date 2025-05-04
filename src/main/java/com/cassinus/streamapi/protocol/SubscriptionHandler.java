package com.cassinus.streamapi.protocol;

import com.cassinus.common.collection.Subscription;
import com.cassinus.common.enums.common.Subscribe;
import com.cassinus.common.enums.messages.ChangeType;
import com.cassinus.common.enums.messages.SegmentType;
import com.cassinus.common.model.message.RequestMessage;
import com.cassinus.common.repository.SubscriptionRepository;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Getter
@Setter
public class SubscriptionHandler<S extends RequestMessage, C extends ChangeMessage<I>, I> {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionHandler.class);
    private final int subscriptionId;
    private final S subscriptionMessage;
    private boolean isSubscribed;
    private final boolean isMergeSegments;
    private List<I> mergedChanges;
    private final StopWatch ttfm;
    private final StopWatch ttlm;
    private int itemCount;
    private final CountDownLatch subscriptionComplete = new CountDownLatch(1);

    private final SubscriptionRepository subscriptionRepository;

    private long lastPublishTime;
    private long lastArrivalTime;
    private String initialClk;
    private String clk;
    private Long heartbeatMs;
    private Long conflationMs;

    public SubscriptionHandler(S subscriptionMessage, boolean isMergeSegments, SubscriptionRepository subscriptionRepository) {
        this.subscriptionRepository = subscriptionRepository;
        this.subscriptionMessage = subscriptionMessage;
        this.isMergeSegments = isMergeSegments;
        isSubscribed = false;
        subscriptionId = subscriptionMessage.getId();
        ttfm = new StopWatch("ttfm");
        ttlm = new StopWatch("ttlm");
        ttfm.start();
        ttlm.start();
    }

    void cancel() {
        // unwind waiters
        subscriptionComplete.countDown();
    }

    public C processChangeMessage(C changeMessage) {
        if (subscriptionId != changeMessage.getId()) {
            // previous subscription id - ignore
            return null;
        }

        // Every message store timings
        lastPublishTime = changeMessage.getPublishTime();
        lastArrivalTime = changeMessage.getArrivalTime();

        if (changeMessage.isStartOfRecovery()) {
            // Start of recovery
            if (ttfm.isRunning()) {
                ttfm.stop();
            }
            LOG.info("{}: Start of image", subscriptionMessage.getOp());
        }

        if (changeMessage.getChangeType() == ChangeType.HEARTBEAT) {
            // Swallow heartbeats
            changeMessage = null;
        } else if (changeMessage.getSegmentType() != SegmentType.NONE && isMergeSegments) {
            // Segmented message and we're instructed to merge (which makes segments look atomic).
            changeMessage = MergeMessage(changeMessage);
        }

        if (changeMessage != null) {
            // store clocks
            if (changeMessage.getInitialClk() != null) {
                initialClk = changeMessage.getInitialClk();
            }

            if (changeMessage.getClk() != null) {
                clk = changeMessage.getClk();
            }

            if (!isSubscribed && changeMessage.getItems() != null) {
                itemCount += changeMessage.getItems().size();
            }

            if (changeMessage.isEndOfRecovery()) {
                // End of recovery
                isSubscribed = true;
                heartbeatMs = changeMessage.getHeartbeatMs();
                conflationMs = changeMessage.getConflateMs();
                if (ttlm.isRunning()) {
                    ttlm.stop();
                }
                LOG.info(
                        "{}: End of image: type:{}, ttfm:{}, ttlm:{}, conflation:{}, heartbeat:{},"
                                + " change.items:{}",
                        subscriptionMessage.getOp(),
                        changeMessage.getChangeType(),
                        ttfm,
                        ttlm,
                        conflationMs,
                        heartbeatMs,
                        itemCount);

                // unwind future
                subscriptionComplete.countDown();
            }

            if ((initialClk != null && !initialClk.isBlank())
                    || (clk != null && !clk.isBlank())) {
                Subscription subscription = new Subscription(Subscribe.SUBSCRIBE.getKey(), initialClk, clk);
                subscriptionRepository.save(subscription);
            }
        }
        return changeMessage;
    }

    private C MergeMessage(C changeMessage) {
        // merge segmented messages so client sees atomic view across segments
        if (changeMessage.getSegmentType() == SegmentType.SEG_START) {
            // start merging
            mergedChanges = new ArrayList<>();
        }
        // accumulate
        mergedChanges.addAll(changeMessage.getItems());

        if (changeMessage.getSegmentType() == SegmentType.SEG_END) {
            // finish merging
            changeMessage.setSegmentType(SegmentType.NONE);
            changeMessage.setItems(mergedChanges);
            mergedChanges = null;
        } else {
            // swallow message as we're still merging
            changeMessage = null;
        }
        return changeMessage;
    }
}
