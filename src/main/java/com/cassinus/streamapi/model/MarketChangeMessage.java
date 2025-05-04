package com.cassinus.streamapi.model;

import com.cassinus.common.model.market.MarketChange;
import com.cassinus.common.model.message.ResponseMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * MarketChangeMessage
 */
public class MarketChangeMessage extends ResponseMessage {
    /**
     * Change Type - set to indicate the type of change - if null this is a delta)
     */
    public enum CtEnum {
        SUB_IMAGE("SUB_IMAGE"),

        RESUB_DELTA("RESUB_DELTA"),

        HEARTBEAT("HEARTBEAT");

        private String value;

        CtEnum(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    private CtEnum ct = null;

    private String clk = null;

    private Long heartbeatMs = null;

    private Long pt = null;

    private String initialClk = null;

    private List<MarketChange> mc = new ArrayList<MarketChange>();

    private Long conflateMs = null;

    /**
     * Segment Type - if the change is split into multiple segments, this denotes the beginning and end of a change, and segments in between. Will be null if data is not segmented
     */
    public enum SegmentTypeEnum {
        SEG_START("SEG_START"),

        SEG("SEG"),

        SEG_END("SEG_END");

        private String value;

        SegmentTypeEnum(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    private SegmentTypeEnum segmentType = null;

    private Integer status = null;

    public MarketChangeMessage ct(CtEnum ct) {
        this.ct = ct;
        return this;
    }

    /**
     * Change Type - set to indicate the type of change - if null this is a delta)
     * @return ct
     **/
    public CtEnum getCt() {
        return ct;
    }

    public void setCt(CtEnum ct) {
        this.ct = ct;
    }

    public MarketChangeMessage clk(String clk) {
        this.clk = clk;
        return this;
    }

    /**
     * Token value (non-null) should be stored and passed in a MarketSubscriptionMessage to resume subscription (in case of disconnect)
     * @return clk
     **/
    public String getClk() {
        return clk;
    }

    public void setClk(String clk) {
        this.clk = clk;
    }

    public MarketChangeMessage heartbeatMs(Long heartbeatMs) {
        this.heartbeatMs = heartbeatMs;
        return this;
    }

    /**
     * Heartbeat Milliseconds - the heartbeat rate (may differ from requested: bounds are 500 to 30000)
     * @return heartbeatMs
     **/
    public Long getHeartbeatMs() {
        return heartbeatMs;
    }

    public void setHeartbeatMs(Long heartbeatMs) {
        this.heartbeatMs = heartbeatMs;
    }

    public MarketChangeMessage pt(Long pt) {
        this.pt = pt;
        return this;
    }

    /**
     * Publish Time (in millis since epoch) that the changes were generated
     * @return pt
     **/
    public Long getPt() {
        return pt;
    }

    public void setPt(Long pt) {
        this.pt = pt;
    }

    public MarketChangeMessage initialClk(String initialClk) {
        this.initialClk = initialClk;
        return this;
    }

    /**
     * Token value (non-null) should be stored and passed in a MarketSubscriptionMessage to resume subscription (in case of disconnect)
     * @return initialClk
     **/
    public String getInitialClk() {
        return initialClk;
    }

    public void setInitialClk(String initialClk) {
        this.initialClk = initialClk;
    }

    public MarketChangeMessage mc(List<MarketChange> mc) {
        this.mc = mc;
        return this;
    }

    public MarketChangeMessage addMcItem(MarketChange mcItem) {
        this.mc.add(mcItem);
        return this;
    }

    /**
     * MarketChanges - the modifications to markets (will be null on a heartbeat
     * @return mc
     **/
    public List<MarketChange> getMc() {
        return mc;
    }

    public void setMc(List<MarketChange> mc) {
        this.mc = mc;
    }

    public MarketChangeMessage conflateMs(Long conflateMs) {
        this.conflateMs = conflateMs;
        return this;
    }

    /**
     * Conflate Milliseconds - the conflation rate (may differ from that requested if subscription is delayed)
     * @return conflateMs
     **/
    public Long getConflateMs() {
        return conflateMs;
    }

    public void setConflateMs(Long conflateMs) {
        this.conflateMs = conflateMs;
    }

    public MarketChangeMessage segmentType(SegmentTypeEnum segmentType) {
        this.segmentType = segmentType;
        return this;
    }

    /**
     * Segment Type - if the change is split into multiple segments, this denotes the beginning and end of a change, and segments in between. Will be null if data is not segmented
     * @return segmentType
     **/
    public SegmentTypeEnum getSegmentType() {
        return segmentType;
    }

    public void setSegmentType(SegmentTypeEnum segmentType) {
        this.segmentType = segmentType;
    }

    public MarketChangeMessage status(Integer status) {
        this.status = status;
        return this;
    }

    /**
     * Stream status: set to null if the exchange stream data is up to date and 503 if the downstream services are experiencing latencies
     * @return status
     **/
    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MarketChangeMessage marketChangeMessage = (MarketChangeMessage) o;
        return Objects.equals(this.ct, marketChangeMessage.ct) &&
                Objects.equals(this.clk, marketChangeMessage.clk) &&
                Objects.equals(this.heartbeatMs, marketChangeMessage.heartbeatMs) &&
                Objects.equals(this.pt, marketChangeMessage.pt) &&
                Objects.equals(this.initialClk, marketChangeMessage.initialClk) &&
                Objects.equals(this.mc, marketChangeMessage.mc) &&
                Objects.equals(this.conflateMs, marketChangeMessage.conflateMs) &&
                Objects.equals(this.segmentType, marketChangeMessage.segmentType) &&
                Objects.equals(this.status, marketChangeMessage.status) &&
                super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ct, clk, heartbeatMs, pt, initialClk, mc, conflateMs, segmentType, status, super.hashCode());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class MarketChangeMessage {\n");
        sb.append("    ").append(toIndentedString(super.toString())).append("\n");
        sb.append("    ct: ").append(toIndentedString(ct)).append("\n");
        sb.append("    clk: ").append(toIndentedString(clk)).append("\n");
        sb.append("    heartbeatMs: ").append(toIndentedString(heartbeatMs)).append("\n");
        sb.append("    pt: ").append(toIndentedString(pt)).append("\n");
        sb.append("    initialClk: ").append(toIndentedString(initialClk)).append("\n");
        sb.append("    mc: ").append(toIndentedString(mc)).append("\n");
        sb.append("    conflateMs: ").append(toIndentedString(conflateMs)).append("\n");
        sb.append("    segmentType: ").append(toIndentedString(segmentType)).append("\n");
        sb.append("    status: ").append(toIndentedString(status)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}

