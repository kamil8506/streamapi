package com.cassinus.streamapi.protocol;

import com.cassinus.common.enums.messages.ConnectionStatus;
import com.cassinus.common.exception.ConnectionException;
import com.cassinus.common.model.market.MarketChange;
import com.cassinus.common.model.message.RequestMessage;
import com.cassinus.common.model.message.ResponseMessage;
import com.cassinus.common.model.message.StatusMessage;
import com.cassinus.common.model.order.OrderChangeMessage;
import com.cassinus.common.model.order.OrderMarketChange;
import com.cassinus.common.model.order.OrderSubscriptionMessage;
import com.cassinus.common.repository.SubscriptionRepository;
import com.cassinus.streamapi.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Getter
@Slf4j
public class RequestResponseProcessor {
    public static final String REQUEST_AUTHENTICATION = "authentication";
    public static final String REQUEST_MARKET_SUBSCRIPTION = "marketSubscription";
    public static final String REQUEST_HEARTBEAT = "heartbeat";

    public static final String RESPONSE_CONNECTION = "connection";
    public static final String RESPONSE_STATUS = "status";
    public static final String RESPONSE_MARKET_CHANGE_MESSAGE = "mcm";
    public static final String RESPONSE_ORDER_CHANGE_MESSAGE = "ocm";

    //private static final Logger LOG = LoggerFactory.getLogger(RequestResponseProcessor.class);
    private final ObjectMapper objectMapper;
    private final AtomicInteger nextId = new AtomicInteger();
    private FutureResponse<ConnectionMessage> connectionMessage = new FutureResponse<>();
    private ConcurrentHashMap<Integer, RequestResponse> tasks = new ConcurrentHashMap<>();

    private final SubscriptionRepository subscriptionRepository;

    // subscription handlers
    private SubscriptionHandler<
            MarketSubscriptionMessage, ChangeMessage<MarketChange>, MarketChange>
            marketSubscriptionHandler;
    private SubscriptionHandler<
            OrderSubscriptionMessage, ChangeMessage<OrderMarketChange>, OrderMarketChange>
            orderSubscriptionHandler;

    private ChangeMessageHandler changeHandler;
    private final RequestSender sendLine;
    private ConnectionStatus status = ConnectionStatus.STOPPED;
    private final CopyOnWriteArrayList<ConnectionStatusListener> connectionStatusListeners =
            new CopyOnWriteArrayList<>();

    private long lastRequestTime = Long.MAX_VALUE;
    private long lastResponseTime = Long.MAX_VALUE;

    private int traceChangeTruncation;
    private final Object sendLock = new Object();

    public RequestResponseProcessor(RequestSender sendLine, SubscriptionRepository subscriptionRepository) {
        this.sendLine = sendLine;
        setChangeHandler(null);
        this.subscriptionRepository = subscriptionRepository;
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        objectMapper.addMixIn(ResponseMessage.class, MixInResponseMessage.class);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private void setStatus(ConnectionStatus value) {
        if (value == status) {
            // no-op
            return;
        }
        ConnectionStatusChangeEvent args = new ConnectionStatusChangeEvent(this, status, value);
        log.info("ESAClient: Status changed {} -> {}", status, value);
        status = value;

        dispatchConnectionStatusChange(args);
    }

    private void dispatchConnectionStatusChange(ConnectionStatusChangeEvent args) {
        try {
            connectionStatusListeners.forEach(c -> c.connectionStatusChange(args));
        } catch (Exception e) {
            log.error("Exception during event dispatch", e);

        }
    }

    private void reset() {
        ConnectionException cancelException =
                new ConnectionException("Connection reset - task cancelled");
        connectionMessage.setException(cancelException);
        connectionMessage = new FutureResponse<>();
        for (RequestResponse task : tasks.values()) {
            task.getFuture().setException(cancelException);
        }
        tasks = new ConcurrentHashMap<>();
    }

    public void disconnected() {
        setStatus(ConnectionStatus.DISCONNECTED);
        reset();
    }

    public void stopped() {
        marketSubscriptionHandler = null;
        orderSubscriptionHandler = null;
        setStatus(ConnectionStatus.STOPPED);
        reset();
    }

    public MarketSubscriptionMessage getMarketResubscribeMessage() {
        if (marketSubscriptionHandler != null) {
            MarketSubscriptionMessage resub = marketSubscriptionHandler.getSubscriptionMessage();
            resub.setInitialClk(marketSubscriptionHandler.getInitialClk());
            resub.setClk(marketSubscriptionHandler.getClk());
            return resub;
        }
        return null;
    }

    public void setChangeHandler(ChangeMessageHandler changeHandler) {
        if (changeHandler == null) changeHandler = new NullChangeHandler();
        this.changeHandler = changeHandler;
    }

    public void setMarketSubscriptionHandler(
            SubscriptionHandler<
                    MarketSubscriptionMessage, ChangeMessage<MarketChange>, MarketChange>
                    newHandler) {
        if (marketSubscriptionHandler != null) marketSubscriptionHandler.cancel();
        marketSubscriptionHandler = newHandler;
        if (marketSubscriptionHandler != null) setStatus(ConnectionStatus.SUBSCRIBED);
    }

    public FutureResponse<StatusMessage> authenticate(AuthenticationMessage message)
            throws ConnectionException {
        header(message, REQUEST_AUTHENTICATION);
        return sendMessage(message, success -> setStatus(ConnectionStatus.AUTHENTICATED));
    }

    public FutureResponse<StatusMessage> heartbeat(HeartbeatMessage message)
            throws ConnectionException {
        header(message, REQUEST_HEARTBEAT);
        return sendMessage(message, null);
    }

    public FutureResponse<StatusMessage> marketSubscription(MarketSubscriptionMessage message)
            throws ConnectionException {
        header(message, REQUEST_MARKET_SUBSCRIPTION);
        SubscriptionHandler<MarketSubscriptionMessage, ChangeMessage<MarketChange>, MarketChange>
                newSub = new SubscriptionHandler<>(message, false, subscriptionRepository);
        return sendMessage(message, success -> setMarketSubscriptionHandler(newSub));
    }

    private FutureResponse<StatusMessage> sendMessage(
            RequestMessage message, Consumer<RequestResponse> onSuccess)
            throws ConnectionException {
        synchronized (sendLock) {
            int id = message.getId();
            RequestResponse requestResponse = new RequestResponse(id, message, onSuccess);

            // store a future task
            tasks.put(id, requestResponse);

            // serialize message & send
            String line;
            try {
                line = objectMapper.writeValueAsString(message);
            } catch (JsonProcessingException e) {
                // should never happen
                throw new ConnectionException("Failed to marshall json", e);
            }
           log.info("Client->ESA: {}", line);

            // send line
            sendLine.sendLine(line);

            // time
            lastRequestTime = System.currentTimeMillis();

            return requestResponse.getFuture();
        }
    }

    private void header(RequestMessage msg, String op) {
        int id = nextId.incrementAndGet();
        msg.setId(id);
        msg.setOp(op);
    }

    public void receiveLine(String line) throws IOException {
        // clear last response
        ResponseMessage message = objectMapper.readValue(line, ResponseMessage.class);
        lastResponseTime = System.currentTimeMillis();
        switch (message.getOp()) {
            case RESPONSE_CONNECTION -> {
               log.info("ESA->Client: {}", line);
                processConnectionMessage((ConnectionMessage) message);
            }
            case RESPONSE_STATUS -> {
                log.info("ESA->Client: {}", line);
                processStatusMessage((StatusMessage) message);
            }
            case RESPONSE_MARKET_CHANGE_MESSAGE -> {
                traceChange(line);
                processMarketChangeMessage((MarketChangeMessage) message);
            }
            case RESPONSE_ORDER_CHANGE_MESSAGE -> {
                traceChange(line);
                processOrderChangeMessage((OrderChangeMessage) message);
            }
            default -> log.error(
                    "ESA->Client: Unknown message type: {}, message:{}", message.getOp(), line);
        }
    }

    private void traceChange(String line) {
        if (traceChangeTruncation != 0) {
            log.info(
                    "ESA->Client: {}",
                    line.substring(0, Math.min(traceChangeTruncation, line.length())));
        }
    }

    private void processOrderChangeMessage(OrderChangeMessage message) {
        ChangeMessage<OrderMarketChange> change = ChangeMessageFactory.ToChangeMessage(message);
        change = orderSubscriptionHandler.processChangeMessage(change);

        if (change != null) changeHandler.onOrderChange(change);
    }

    private void processMarketChangeMessage(MarketChangeMessage message) {
        ChangeMessage<MarketChange> change = ChangeMessageFactory.ToChangeMessage(message);
        change = marketSubscriptionHandler.processChangeMessage(change);
        if (change != null) changeHandler.onMarketChange(change);
    }

    private void processStatusMessage(StatusMessage statusMessage) {
        if (statusMessage.getId() == null) {
            // async status / status for a message that couldn't be decoded
            processUncorrelatedStatus(statusMessage);
        } else {
            RequestResponse task = tasks.get(statusMessage.getId());
            if (task == null) {
                // shouldn't happen
                processUncorrelatedStatus(statusMessage);
            } else {
                // unwind task
                task.processStatusMessage(statusMessage);
            }
        }
    }

    private void processUncorrelatedStatus(StatusMessage statusMessage) {
        log.error("Error Status Notification: {}", statusMessage);
        changeHandler.onErrorStatusNotification(statusMessage);
    }

    private void processConnectionMessage(ConnectionMessage message) {
        connectionMessage.setResponse(message);
        setStatus(ConnectionStatus.CONNECTED);
    }

    public static class NullChangeHandler implements ChangeMessageHandler {
        private static final Logger LOG = LoggerFactory.getLogger(NullChangeHandler.class);

        @Override
        public void onOrderChange(ChangeMessage<OrderMarketChange> change) {
            LOG.info("onOrderChange: {}", change);
        }

        @Override
        public void onMarketChange(ChangeMessage<MarketChange> change) {
            LOG.info("onMarketChange: {}", change);
        }

        @Override
        public void onErrorStatusNotification(StatusMessage message) {
            log.info("onErrorStatusNotification: {}", message);
        }
    }
}
