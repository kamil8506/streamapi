package com.cassinus.streamapi.service;

import com.cassinus.common.model.market.MarketChange;
import com.cassinus.common.model.order.OrderMarketChange;
import com.cassinus.common.model.message.StatusMessage;
import com.cassinus.streamapi.market.MarketCache;
import com.cassinus.streamapi.protocol.ChangeMessage;
import com.cassinus.streamapi.protocol.ChangeMessageHandler;

import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Slf4j
@Service
public class StreamCache implements ChangeMessageHandler {
    private final MarketCache marketCache;
    private final Logger logger = Logger.getLogger(StreamCache.class.getName());

    @Autowired
    public StreamCache(MarketCache marketCache) {
        this.marketCache = marketCache;
    }

    public void onOrderChange(ChangeMessage<OrderMarketChange> change) {
        // will provide the code later
    log.info("On Order Change = " + change.toString());

    }

    @Override
    public void onMarketChange(ChangeMessage<MarketChange> change) {
        marketCache.onMarketChange(change);
    }

    @Override
    public void onErrorStatusNotification(StatusMessage message) {
        //will provide the code later
        log.info("On ErrorStatus Notification = " + message.toString());
    }
}