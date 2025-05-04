package com.cassinus.streamapi.protocol;

import com.cassinus.common.enums.messages.ChangeType;
import com.cassinus.common.enums.messages.SegmentType;
import lombok.*;

import java.util.List;

@Data
@AllArgsConstructor
public class ChangeMessage<T> {
    private long arrivalTime;
    private long publishTime;
    private int id;
    private String clk;
    private String initialClk;
    private Long heartbeatMs;
    private Long conflateMs;
    private List<T> items;
    private SegmentType segmentType;

    private ChangeType changeType;

    public ChangeMessage() {
        arrivalTime = System.currentTimeMillis();
    }

    public boolean isStartOfNewSubscription() {
        return changeType == ChangeType.SUB_IMAGE
                && (segmentType == SegmentType.NONE || segmentType == SegmentType.SEG_START);
    }

    public boolean isStartOfRecovery() {
        return (changeType == ChangeType.SUB_IMAGE || changeType == ChangeType.RESUB_DELTA)
                && (segmentType == SegmentType.NONE || segmentType == SegmentType.SEG_START);
    }

    public boolean isEndOfRecovery() {
        return (changeType == ChangeType.SUB_IMAGE || changeType == ChangeType.RESUB_DELTA)
                && (segmentType == SegmentType.NONE || segmentType == SegmentType.SEG_END);
    }
}