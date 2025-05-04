package com.cassinus.streamapi.model;

import com.cassinus.common.model.market.MarketDataFilter;
import com.cassinus.common.model.market.MarketFilter;
import com.cassinus.common.model.message.RequestMessage;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Objects;

@Getter
@Setter
@Document
public class MarketSubscriptionMessage extends RequestMessage {
    private Boolean segmentationEnabled = null;

    private String clk = null;

    private Long heartbeatMs = null;

    private String initialClk = null;

    private MarketFilter marketFilter = null;

    private Long conflateMs = null;

    private MarketDataFilter marketDataFilter = null;

    public MarketSubscriptionMessage segmentationEnabled(Boolean segmentationEnabled) {
        this.segmentationEnabled = segmentationEnabled;
        return this;
    }

    public MarketSubscriptionMessage clk(String clk) {
        this.clk = clk;
        return this;
    }

    public MarketSubscriptionMessage heartbeatMs(Long heartbeatMs) {
        this.heartbeatMs = heartbeatMs;
        return this;
    }

    public MarketSubscriptionMessage initialClk(String initialClk) {
        this.initialClk = initialClk;
        return this;
    }

    public MarketSubscriptionMessage marketFilter(MarketFilter marketFilter) {
        this.marketFilter = marketFilter;
        return this;
    }

    public MarketSubscriptionMessage conflateMs(Long conflateMs) {
        this.conflateMs = conflateMs;
        return this;
    }

    public MarketSubscriptionMessage marketDataFilter(MarketDataFilter marketDataFilter) {
        this.marketDataFilter = marketDataFilter;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MarketSubscriptionMessage marketSubscriptionMessage = (MarketSubscriptionMessage) o;
        return Objects.equals(this.segmentationEnabled, marketSubscriptionMessage.segmentationEnabled) &&
                Objects.equals(this.clk, marketSubscriptionMessage.clk) &&
                Objects.equals(this.heartbeatMs, marketSubscriptionMessage.heartbeatMs) &&
                Objects.equals(this.initialClk, marketSubscriptionMessage.initialClk) &&
                Objects.equals(this.marketFilter, marketSubscriptionMessage.marketFilter) &&
                Objects.equals(this.conflateMs, marketSubscriptionMessage.conflateMs) &&
                Objects.equals(this.marketDataFilter, marketSubscriptionMessage.marketDataFilter) &&
                super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentationEnabled, clk, heartbeatMs, initialClk, marketFilter, conflateMs, marketDataFilter, super.hashCode());
    }

    @Override
    public String toString() {
        return "class MarketSubscriptionMessage {\n" +
                "    " + toIndentedString(super.toString()) + "\n" +
                "    segmentationEnabled: " + toIndentedString(segmentationEnabled) + "\n" +
                "    clk: " + toIndentedString(clk) + "\n" +
                "    heartbeatMs: " + toIndentedString(heartbeatMs) + "\n" +
                "    initialClk: " + toIndentedString(initialClk) + "\n" +
                "    marketFilter: " + toIndentedString(marketFilter) + "\n" +
                "    conflateMs: " + toIndentedString(conflateMs) + "\n" +
                "    marketDataFilter: " + toIndentedString(marketDataFilter) + "\n" +
                "}";
    }

    private String toIndentedString(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}