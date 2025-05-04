package com.cassinus.streamapi.protocol;


import com.cassinus.common.model.market.MarketChange;
import com.cassinus.common.model.order.OrderMarketChange;
import com.cassinus.common.model.message.StatusMessage;

public interface ChangeMessageHandler {
    void onOrderChange(ChangeMessage<OrderMarketChange> change);

    void onMarketChange(ChangeMessage<MarketChange> change);

    void onErrorStatusNotification(StatusMessage message);
}
