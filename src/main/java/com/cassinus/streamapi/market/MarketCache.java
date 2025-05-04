package com.cassinus.streamapi.market;

import com.cassinus.common.collection.*;
import com.cassinus.common.enums.common.OperationType;
import com.cassinus.common.enums.common.RedisNameSpace;
import com.cassinus.common.enums.market.MarketStatus;
import com.cassinus.common.model.market.MarketChange;
import com.cassinus.common.model.market.MarketRunnerPrices;
import com.cassinus.common.model.market.MarketSnap;
import com.cassinus.common.repository.*;
import com.cassinus.common.service.MarketCountService;
import com.cassinus.streamapi.protocol.ChangeMessage;
import com.cassinus.streamapi.service.PublishService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class MarketCache {
    private static final Logger logger = LoggerFactory.getLogger(MarketCache.class);

    @Value("${stream.isMarketRemovedOnClose}")
    private boolean isMarketRemovedOnClose;

    private int conflatedCount;
    private final EventDataRepository eventDataRepository;
    private final MatchDataRepository matchDataRepository;
    private final SportDataRepository sportDataRepository;
    private final MarketDataRepository marketDataRepository;
    private final MarketCountService marketCountService;

    private final RedisTemplate<String, Object> redisTemplate;
    private final PublishService publishService;
    private final ObjectMapper objectMapper;

    private final CopyOnWriteArrayList<MarketChangeListener> marketChangeListeners =
            new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<BatchMarketsChangeListener> batchMarketChangeListeners =
            new CopyOnWriteArrayList<>();

    @Autowired
    public MarketCache(RedisTemplate<String, Object> redisTemplate, ObjectMapper objectMapper
            , MarketDataRepository marketDataRepository, PublishService publishService
            , EventDataRepository eventDataRepository, MatchDataRepository matchDataRepository
            , SportDataRepository sportDataRepository, MarketCountService marketCountService) {
        this.redisTemplate = redisTemplate;
        this.marketDataRepository = marketDataRepository;
        this.publishService = publishService;
        this.objectMapper = objectMapper;
        this.eventDataRepository = eventDataRepository;
        this.matchDataRepository = matchDataRepository;
        this.sportDataRepository = sportDataRepository;
        this.marketCountService = marketCountService;
    }

    public void onMarketChange(ChangeMessage<MarketChange> changeMessage) {
        //System.out.println("Change message = "+ changeMessage);
        if (changeMessage.isStartOfNewSubscription()) {
            clearCacheAndDb();
        }

        if (changeMessage.getItems() != null) {
            // lazy build events
            List<MarketChangeEvent> batch =
                    (batchMarketChangeListeners.isEmpty())
                            ? null
                            : new ArrayList<>(changeMessage.getItems().size());

            for (MarketChange marketChange : changeMessage.getItems()) {
                boolean isDataAvailable = isDataAvailable(RedisNameSpace.MARKET_DATA.getName(), marketChange.getId());

                if (isDataAvailable) {
                    Market market = onMarketChange(marketChange);
                    MarketData marketData = getDataFromCache(RedisNameSpace.MARKET_DATA.getName(), market.getMarketId(), MarketData.class);

                    if (isMarketRemovedOnClose && market.isClosed()
                            && marketData != null) {

                        logger.info("market closed: {}", market.isClosed());
                        processMarketUpdate(market, marketData, true);

                    } else if (marketData != null) {

                        Boolean oldInplay = marketData.isInPlay();

                        Map<String, Object> changes = updateMarketChanges(market, marketData);
                        changes.forEach((key, value) -> logger.info("Change detected: {} = {}", key, value));

                        Boolean newInplay = marketData.isInPlay();

                        if (Boolean.FALSE.equals(oldInplay) && Boolean.TRUE.equals(newInplay)) {
                            logger.info("Type Changes: , oldInPlayType: {}, new InPLayType: {}", oldInplay, newInplay);
                            processInplayChanges(marketData.getSportId(), marketData.getEventId(), marketData.getMatchId());
                        }

                        processMarketUpdate(market, marketData, false);
                    }

                    // lazy build events
                    if (batch != null || !marketChangeListeners.isEmpty()) {
                        MarketChangeEvent marketChangeEvent = new MarketChangeEvent(this);
                        marketChangeEvent.setChange(marketChange);
                        marketChangeEvent.setMarket(market);
                        dispatchMarketChanged(marketChangeEvent);
                        if (batch != null) {
                            batch.add(marketChangeEvent);
                        }
                    }
                }
                if (batch != null) {
                    dispatchBatchMarketChanged(new BatchMarketChangeEvent(batch));
                }
            }
        }
    }

    private void processInplayChanges(int sportId, long eventId, long matchId) {
        processMatchInplayChanges(sportId, eventId, matchId);
        processSportsInplayChanges(sportId, matchId);
    }

    private void processSportsInplayChanges(int sportId, long matchId) {
        SportData sportData = getDataFromCache(RedisNameSpace.SPORT_DATA.getName(), String.valueOf(sportId), SportData.class);
        if (sportData != null) {
            if (sportData.getIsLive().get() != 1) {
                sportData.getIsLive().set(1);
            }

            if (sportData.getLiveMatchIds().add(matchId)) {
                sportData.incrementLiveCount(); // custom method to increment
                logger.info("Live match count increased for sportId={}, matchId={}", sportId, matchId);
            }

            redisTemplate.opsForHash().put(RedisNameSpace.SPORT_DATA.getName(), String.valueOf(sportId), sportData);

            publishService.publishDataToGame(sportData, RedisNameSpace.SPORT_UPDATE.getName(), OperationType.UPDATE);
            sportDataRepository.save(sportData);
        }
    }

    private void processMatchInplayChanges(int sportId, long eventId, long matchId) {
        MatchData matchData = getDataFromCache(RedisNameSpace.MATCH_DATA.getName()
                , String.valueOf(sportId) + eventId + matchId, MatchData.class);
        if (matchData == null) {
            logger.warn("MatchData not found for matchId={}", matchId);
            return;
        }
        if (matchData.getType() == 1) {
            logger.debug("Match {} already in-play (type 1), skipping promotion", matchId);
            return;
        }

        if (matchData.getType() == 2) {
            //remove the old publish data
            publishService.publishDataToGame(matchData, RedisNameSpace.MATCH_UPDATE.getName(), OperationType.REMOVE);

            matchData.setType(1); // Promote to type 1
            matchData.setUpdatedDate(LocalDateTime.now());
            String matchIdStr = String.valueOf(matchId); // Convert Long to String
            // Save match data update to Redis and DB
            redisTemplate.opsForHash().put(RedisNameSpace.MATCH_DATA.getName(), String.valueOf(sportId) + eventId + matchId, matchData);
            matchDataRepository.save(matchData);

            // publish the new one
            publishService.publishDataToGame(matchData, RedisNameSpace.MATCH_UPDATE.getName(), OperationType.UPDATE);
        }
    }

    private void processMarketUpdate(Market market, MarketData marketData, boolean isRemove) {
        if (isRemove) {
            publishService.publishDataToGame(marketData, RedisNameSpace.MARKET_UPDATE.getName(), OperationType.REMOVE);

            redisTemplate.opsForHash().delete(RedisNameSpace.MARKET_DATA.getName(), marketData.getMarketId());
            marketDataRepository.deleteByMarketId(marketData.getMarketId());

            redisTemplate.opsForHash().delete(RedisNameSpace.MARKET.getName(), marketData.getMarketId());

            decrementMarketCount(marketData, (marketData.getSportId() == 7 || marketData.getSportId() == 4339));
        } else {
            // update marketData cache and db
            redisTemplate.opsForHash().put(RedisNameSpace.MARKET_DATA.getName(), marketData.getMarketId(), marketData);
            publishService.publishDataToGame(marketData, RedisNameSpace.MARKET_UPDATE.getName(), OperationType.UPDATE);
            marketDataRepository.save(marketData);

            // update market cache and db
            redisTemplate.opsForHash().put(RedisNameSpace.MARKET.getName(), marketData.getMarketId(), market);
        }
    }

    private void decrementMarketCount(MarketData marketData, boolean isRacingSport) {
        logger.info("sport: {}, event: {}, match: {}", marketData.getSportId(), marketData.getEventId(), marketData.getMatchId());

        MatchData matchData = getDataFromCache(RedisNameSpace.MATCH_DATA.getName()
                , String.valueOf(marketData.getSportId()) + marketData.getEventId()
                        + marketData.getMatchId(), MatchData.class);

        EventData eventData = getDataFromCache(RedisNameSpace.EVENT_DATA.getName()
                , String.valueOf(marketData.getSportId()) + marketData.getEventId(), EventData.class);

        SportData sportData = getDataFromCache(RedisNameSpace.SPORT_DATA.getName()
                , String.valueOf(marketData.getSportId()), SportData.class);

        if (matchData != null) {
            marketCountService.decrementMatchMarketCount(matchData.getSportId(), matchData.getEventId(), matchData.getMatchId());
            matchData.setMarketCount(new AtomicInteger(marketCountService.getCount("MATCH",
                    String.valueOf(matchData.getSportId()) + matchData.getEventId() + matchData.getMatchId())));
        }

        if (!isRacingSport && eventData != null) {
            marketCountService.decrementEventMarketCount(eventData.getSportId(), eventData.getEventId());
            eventData.setMarketCount(new AtomicInteger(marketCountService.getCount("EVENT",
                    String.valueOf(eventData.getSportId()) + eventData.getEventId())));
        }

        if (sportData != null) {
            marketCountService.decrementSportMarketCount(sportData.getSportId());
            sportData.setMarketCount(new AtomicInteger(marketCountService.getCount("SPORT",
                    String.valueOf(sportData.getSportId()))));
        }

        setMarketCount(sportData, eventData, matchData, isRacingSport);
       // processSportsUpdate(matchData, sportData);

        processStoreAndPublishData(sportData, eventData, matchData);
    }

    private void processSportsUpdate(MatchData matchData, SportData sportData) {
        if (matchData != null) {
            long matchId = matchData.getMatchId();

            boolean isMarketAvailable = isMarketAvailableForMatch(matchId);

            if (!isMarketAvailable) {
              //  marketCountService.decrementSportLiveCount();
                int liveMatchAvailable = getMatchListCountAfterRemoval(RedisNameSpace.SPORT_LIVE_MATCH_ID.getName(), String.valueOf(sportData.getSportId()), matchData.getMatchId());
                if (liveMatchAvailable == 0) {
                    populateLiveMatchIds(sportData);
                    sportData.setLiveMatchIds(ConcurrentHashMap.newKeySet());
                }
            }
        }
    }

    public void populateLiveMatchIds(SportData sportData) {
        String redisKey = RedisNameSpace.SPORT_LIVE_MATCH_ID.getName();
        String sportKey = String.valueOf(sportData.getSportId());
        Object matchSetObject = redisTemplate.opsForHash().get(redisKey, sportKey);

        if (matchSetObject != null) {
            try {
                String matchSetJson = matchSetObject.toString();
                Set<Long> matchIds = objectMapper.readValue(matchSetJson, new TypeReference<>() {
                });
                sportData.setLiveMatchIds(matchIds);
            } catch (Exception e) {
                logger.error("Failed to decode match IDs from Redis for sportId: {}", sportData.getSportId(), e);
                sportData.setLiveMatchIds(ConcurrentHashMap.newKeySet());
            }
        } else {
            sportData.setLiveMatchIds(ConcurrentHashMap.newKeySet());
        }
    }

    private boolean isMarketAvailableForMatch(long matchId) {
        Map<Object, Object> marketDataMap = redisTemplate.opsForHash().entries("marketData");
        return marketDataMap.values().stream()
                .anyMatch(value -> {
                    try {
                        MarketData marketData = objectMapper.readValue(value.toString(), MarketData.class);
                        return marketData.getMatchId() == matchId;
                    } catch (Exception e) {
                        e.printStackTrace(); // Handle deserialization errors
                        return false;
                    }
                });
    }

    private void clearCacheAndDb() {
        clearCacheData();
        cleaDbData();
    }

    private void clearCacheData() {
        redisTemplate.delete(RedisNameSpace.MARKET_DATA.getName());
        redisTemplate.delete(RedisNameSpace.EVENT_DATA.getName());
        redisTemplate.delete(RedisNameSpace.MATCH_DATA.getName());
        redisTemplate.delete(RedisNameSpace.MARKET.getName());
        redisTemplate.delete(RedisNameSpace.MARKET_COUNT.getName());
    }

    private void cleaDbData() {
        marketDataRepository.deleteAll();
        eventDataRepository.deleteAll();
        matchDataRepository.deleteAll();
    }

    private boolean isDataAvailable(String key, String hashKey) {
        return redisTemplate.opsForHash().hasKey(key, hashKey);
    }

    public <T> T getDataFromCache(String key, String hashKey, Class<T> clazz) {
        Object result = redisTemplate.opsForHash().get(key, hashKey);
        if (result == null) {
            return null;
        }
        return objectMapper.convertValue(result, clazz);
    }

    private Market onMarketChange(MarketChange marketChange) {
        if (Boolean.TRUE.equals(marketChange.getCon())) {
            conflatedCount++;
            logger.info("conflated count: {}", conflatedCount);
        }

        Market market = getDataFromCache(RedisNameSpace.MARKET.getName(), marketChange.getId(), Market.class);
        market = market == null ? new Market(marketChange.getId()) : market;
        market.onMarketChange(marketChange);
        return market;
    }

    private Map<String, Object> updateMarketChanges(Market market, MarketData marketData) {
        Map<String, Object> changes = new HashMap<>();
        // Update market definition data if present
        Optional.ofNullable(market)
                .map(Market::getMarketDefinition)
                .ifPresent(def -> {
                    Optional.ofNullable(def.getInPlay())
                            .ifPresent(inPlay -> {
                                marketData.setInPlay(inPlay);
                                changes.put("inPlay", inPlay);
                            });

                    Optional.ofNullable(def.getStatus())
                            .map(MarketStatus::getValue)
                            .ifPresent(status -> {
                                marketData.setStatus(status);
                                changes.put("status", status);
                            });

                    Optional.ofNullable(def.getTurnInPlayEnabled())
                            .ifPresent(turnInPlayEnabled -> {
                                marketData.setTurnInplayEnabled(turnInPlayEnabled);
                                changes.put("turnInPlayEnabled", turnInPlayEnabled);
                            });
                });

        // Update traded volume if present and not zero
        Optional.ofNullable(market)
                .map(Market::getTv)
                .filter(tv -> tv != 0.0)
                .ifPresent(tradedVolume -> {
                    marketData.setTradedVolume(tradedVolume);
                    changes.put("tradedVolume", tradedVolume);
                });

        Optional.ofNullable(market)
                .map(Market::getSnap)
                .map(MarketSnap::getMarketRunners)
                .ifPresent(snaps -> snaps.forEach(snap ->
                        marketData.getRunners().stream()
                                .filter(r -> r.getRunnerId() == snap.getRunnerId().getSelectionId())
                                .findFirst()
                                .ifPresent(runner -> {
                                    MarketRunnerPrices prices = snap.getPrices();
                                    if (prices != null) {
                                        if (prices.getBdatb() != null && !prices.getBdatb().isEmpty()) {
                                            runner.setAvailableToBack(prices.getBdatb());
                                            changes.put("bdatb_" + snap.getRunnerId(), prices.getBdatb());
                                        }
                                        if (prices.getBdatl() != null && !prices.getBdatl().isEmpty()) {
                                            runner.setAvailableToLay(prices.getBdatl());
                                            changes.put("bdatl_" + snap.getRunnerId(), prices.getBdatl());
                                        }
                                    }
                                })
                ));
        return changes;
    }

    // Event for each market change
    private void dispatchMarketChanged(MarketChangeEvent marketChangeEvent) {
        try {
            marketChangeListeners.forEach(l -> l.marketChange(marketChangeEvent));
        } catch (Exception e) {
            logger.error("Exception from event listener", e);
        }
    }

    // Event for each batch of market changes
    // (note to be truly atomic you will want to set to merge segments otherwise an event could be
    // segmented)
    private void dispatchBatchMarketChanged(BatchMarketChangeEvent batchMarketChangeEvent) {
        try {
            batchMarketChangeListeners.forEach(l -> l.batchMarketsChange(batchMarketChangeEvent));
        } catch (Exception e) {
            logger.error("Exception from batch event listener", e);
        }
    }

    // Listeners
    public static class MarketChangeEvent extends EventObject {
        // the raw change message that was just applied
        private MarketChange change;
        // the market changed - this is reference invariant
        private Market market;

        /**
         * Constructs a prototypical Event.
         *
         * @param source The object on which the Event initially occurred.
         * @throws IllegalArgumentException if source is null.
         */
        public MarketChangeEvent(Object source) {
            super(source);
        }

        void setChange(MarketChange change) {
            this.change = change;
        }

        void setMarket(Market market) {
            this.market = market;
        }

    }

    public static class BatchMarketChangeEvent extends EventObject {
        /**
         * Constructs a prototypical Event.
         *
         * @param source The object on which the Event initially occurred.
         * @throws IllegalArgumentException if source is null.
         */
        public BatchMarketChangeEvent(Object source) {
            super(source);
        }

    }

    public interface MarketChangeListener extends java.util.EventListener {
        void marketChange(MarketChangeEvent marketChangeEvent);
    }

    public interface BatchMarketsChangeListener extends java.util.EventListener {
        void batchMarketsChange(BatchMarketChangeEvent batchMarketChangeEvent);
    }

    @SafeVarargs
    public final <T> void processStoreAndPublishData(T... data) {
        if (data == null) {
            return; // Early return if the entire array is null
        }
        for (T updatedData : data) {
            if (updatedData == null) {
                continue; // Skip this iteration if individual data item is null
            }

            switch (updatedData) { // 7 - horse racing 4339 - greyhound racing
                case EventData eventData when eventData.getSportId() != 7 && eventData.getSportId() != 4339 ->
                        storeAndPublishData(eventData, RedisNameSpace.EVENT_UPDATE.getName(), RedisNameSpace.EVENT_DATA.getName(),
                                String.valueOf(eventData.getSportId()) + eventData.getEventId(), eventDataRepository);
                case MatchData matchData ->
                        storeAndPublishData(matchData, RedisNameSpace.MATCH_UPDATE.getName(), RedisNameSpace.MATCH_DATA.getName(),
                                String.valueOf(matchData.getSportId()) + matchData.getEventId() + matchData.getMatchId(), matchDataRepository);
                case MarketData marketData ->
                        storeAndPublishData(marketData, RedisNameSpace.MARKET_UPDATE.getName(), RedisNameSpace.MARKET_DATA.getName(), marketData.getMarketId()
                                , marketDataRepository);
                case SportData sportData ->
                        storeAndPublishData(sportData, RedisNameSpace.SPORT_UPDATE.getName(), RedisNameSpace.SPORT_DATA.getName(),
                                String.valueOf(sportData.getSportId()), sportDataRepository);
                default -> throw new IllegalArgumentException("Unsupported data type: " + data.getClass().getName());
            }
        }
    }


    private <T> void storeAndPublishData(T data, String topic, String cacheKey, String hashKey
            , MongoRepository<T, ?> repository) {
        //save to cache
        redisTemplate.opsForHash().put(cacheKey, hashKey, data);
        //save to db
        repository.save(data);
        // publish to game
        publishService.publishDataToGame(data, topic, OperationType.UPDATE);
    }

    private void setMarketCount(SportData sportData, EventData eventData, MatchData matchData, boolean isRacingSport) {
        if (sportData != null) {
            sportData.setMarketCount(new AtomicInteger(marketCountService.getCount("SPORT",
                    String.valueOf(sportData.getSportId()))));
        }

        if (eventData != null && !isRacingSport) {
            eventData.setMarketCount(new AtomicInteger(marketCountService.getCount("EVENT",
                    String.valueOf(eventData.getSportId()) + eventData.getEventId())));
        }

        if (matchData != null) {
            matchData.setMarketCount(new AtomicInteger(marketCountService.getCount("MATCH",
                    String.valueOf(matchData.getSportId()) + matchData.getEventId() + matchData.getMatchId())));
        }
    }

    private int getMatchListCountAfterRemoval(String redisKey, String hashKey, long matchId) {
        String script =
                "local eventList = redis.call('hget', KEYS[1], KEYS[2]) " +
                        "if not eventList then " +
                        "   return 0 " +
                        "end " +
                        "local eventTable = cjson.decode(eventList) " +
                        "for i, id in ipairs(eventTable) do " +
                        "   if id == tonumber(ARGV[1]) then " +
                        "       table.remove(eventTable, i) " +
                        "       break " +
                        "   end " +
                        "end " +
                        "redis.call('hset', KEYS[1], KEYS[2], cjson.encode(eventTable)) " +
                        "return #eventTable";

        Long result = redisTemplate.execute(
                new DefaultRedisScript<>(script, Long.class),
                List.of(redisKey, hashKey),
                matchId
        );

        return result != null ? result.intValue() : 0;
    }
}