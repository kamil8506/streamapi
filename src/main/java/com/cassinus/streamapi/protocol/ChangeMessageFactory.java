package com.cassinus.streamapi.protocol;


import com.cassinus.common.enums.messages.ChangeType;
import com.cassinus.common.enums.messages.SegmentType;
import com.cassinus.common.model.market.MarketChange;
import com.cassinus.streamapi.model.MarketChangeMessage;
import com.cassinus.common.model.order.OrderChangeMessage;
import com.cassinus.common.model.order.OrderMarketChange;

public class ChangeMessageFactory {

    public static ChangeMessage<MarketChange> ToChangeMessage(MarketChangeMessage message) {
        ChangeMessage<MarketChange> change = new ChangeMessage<>();
        change.setId(message.getId());
        change.setPublishTime(message.getPt());
        change.setClk(message.getClk());
        change.setInitialClk(message.getInitialClk());
        change.setConflateMs(message.getConflateMs());
        change.setHeartbeatMs(message.getHeartbeatMs());

        change.setItems(message.getMc());

        SegmentType segmentType = SegmentType.NONE;
        if (message.getSegmentType() != null) {
            switch (message.getSegmentType()) {
                case SEG_START -> segmentType = SegmentType.SEG_START;
                case SEG_END -> segmentType = SegmentType.SEG_END;
                case SEG -> segmentType = SegmentType.SEG;
            }
        }
        change.setSegmentType(segmentType);

        ChangeType changeType = ChangeType.UPDATE;
        if (message.getCt() != null) {
            switch (message.getCt()) {
                case HEARTBEAT -> changeType = ChangeType.HEARTBEAT;
                case RESUB_DELTA -> changeType = ChangeType.RESUB_DELTA;
                case SUB_IMAGE -> changeType = ChangeType.SUB_IMAGE;
            }
        }
        change.setChangeType(changeType);

        return change;
    }

    public static ChangeMessage<OrderMarketChange> ToChangeMessage(OrderChangeMessage message) {
        ChangeMessage<OrderMarketChange> change = new ChangeMessage<>();
        change.setId(message.getId());
        change.setPublishTime(message.getPt());
        change.setClk(message.getClk());
        change.setInitialClk(message.getInitialClk());
        change.setConflateMs(message.getConflateMs());
        change.setHeartbeatMs(message.getHeartbeatMs());

        change.setItems(message.getOc());

        SegmentType segmentType = SegmentType.NONE;
        if (message.getSegmentType() != null) {
            switch (message.getSegmentType()) {
                case SEG_START -> segmentType = SegmentType.SEG_START;
                case SEG_END -> segmentType = SegmentType.SEG_END;
                case SEG -> segmentType = SegmentType.SEG;
            }
        }
        change.setSegmentType(segmentType);

        ChangeType changeType = ChangeType.UPDATE;
        if (message.getCt() != null) {
            switch (message.getCt()) {
                case HEARTBEAT -> changeType = ChangeType.HEARTBEAT;
                case RESUB_DELTA -> changeType = ChangeType.RESUB_DELTA;
                case SUB_IMAGE -> changeType = ChangeType.SUB_IMAGE;
            }
        }
        change.setChangeType(changeType);

        return change;
    }
}