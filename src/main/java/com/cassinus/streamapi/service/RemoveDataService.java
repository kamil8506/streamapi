package com.cassinus.streamapi.service;

import com.cassinus.common.collection.EventData;
import com.cassinus.common.collection.MatchData;
import com.cassinus.common.enums.common.OperationType;
import com.cassinus.common.enums.common.RedisNameSpace;
import com.cassinus.common.repository.EventDataRepository;
import com.cassinus.common.repository.MatchDataRepository;
import com.cassinus.common.service.MarketCountService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class RemoveDataService {

    private final Logger log = LoggerFactory.getLogger(RemoveDataService.class);

    private final MatchDataRepository matchDataRepository;
    private final EventDataRepository eventDataRepository;
    private final MarketCountService marketCountService;
    private final PublishService publishService;
    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public RemoveDataService(MatchDataRepository matchDataRepository, ObjectMapper objectMapper
            , EventDataRepository eventDataRepository, RedisTemplate<String, Object> redisTemplate
            , PublishService publishService, MarketCountService marketCountService) {
        this.matchDataRepository = matchDataRepository;
        this.objectMapper = objectMapper;
        this.eventDataRepository = eventDataRepository;
        this.redisTemplate = redisTemplate;
        this.publishService = publishService;
        this.marketCountService = marketCountService;
    }

    @Scheduled(
            initialDelayString = "${scheduled.initialDelay}",
            fixedRateString = "${scheduled.fixedRate}"
    )
    private void removeCompletedData() {
        Set<Object> sportKeysObj = redisTemplate.opsForHash().keys(RedisNameSpace.SPORT_DATA.getName());
        Set<Object> eventKeysObj = redisTemplate.opsForHash().keys(RedisNameSpace.EVENT_DATA.getName());
        Set<Object> matchKeysObj = redisTemplate.opsForHash().keys(RedisNameSpace.MATCH_DATA.getName());
        Set<Object> marketKeysObj = redisTemplate.opsForHash().keys(RedisNameSpace.MARKET_DATA.getName());

        Set<String> sportKeys = sportKeysObj.stream().map(Object::toString).collect(Collectors.toSet());
        Set<String> eventKeys = eventKeysObj.stream().map(Object::toString).collect(Collectors.toSet());
        Set<String> matchKeys = matchKeysObj.stream().map(Object::toString).collect(Collectors.toSet());
        Set<String> marketKeys = marketKeysObj.stream().map(Object::toString).collect(Collectors.toSet());

        Map<String, Set<String>> matchToMarketsMap = new HashMap<>();
        for (String marketId : marketKeys) {
            Object marketDataObj = redisTemplate.opsForHash().get(RedisNameSpace.MARKET_DATA.getName(), marketId);
            if (marketDataObj != null) {
                String matchKey = extractMatchKeyFromMarketData(marketDataObj);
                if (matchKey != null) {
                    matchToMarketsMap.computeIfAbsent(matchKey, k -> new HashSet<>()).add(marketId);
                }
            }
        }

        for (String sportId : sportKeys) {
            boolean isRacingSport = sportId.equals("7") || sportId.equals("4339");
            for (String eventKey : eventKeys) {
                if (!eventKey.startsWith(sportId)) continue;
                String eventId = eventKey.substring(sportId.length());
                String matchPrefix = sportId + eventId;

                for (String matchKey : matchKeys) {
                    if (!matchKey.startsWith(matchPrefix)) continue;
                    String matchId = matchKey.substring(matchPrefix.length());

                    boolean hasMarket = matchToMarketsMap.containsKey(matchKey)
                            && !matchToMarketsMap.get(matchKey).isEmpty();

                    if (!hasMarket) {
                        MatchData matchData = getDataFromCache(RedisNameSpace.MATCH_DATA.getName(), matchKey, MatchData.class);
                        processMatchRemoval(matchData, matchId, matchKey);
                    }

                    boolean hasMatch = redisTemplate.opsForHash().keys(RedisNameSpace.MATCH_DATA.getName()).stream()
                            .map(Object::toString)
                            .anyMatch(match -> match.startsWith(matchPrefix));

                    if (!hasMatch && !isRacingSport) {
                        EventData eventData = getDataFromCache(RedisNameSpace.EVENT_DATA.getName(), eventKey, EventData.class);
                        processEventRemoval(eventData, eventId, eventKey);
                    }
                }
            }
        }
    }

    private String extractMatchKeyFromMarketData(Object marketDataObj) {
        try {
            Map<String, Object> marketMap = objectMapper.convertValue(marketDataObj, Map.class);
            if (marketMap.containsKey("sportId") && marketMap.containsKey("eventId") && marketMap.containsKey("matchId")) {
                String sportId = marketMap.get("sportId").toString();
                String eventId = marketMap.get("eventId").toString();
                String matchId = marketMap.get("matchId").toString();
                return sportId + eventId + matchId;
            }
            return null;
        } catch (Exception e) {
            log.error("Error extracting match key from market data", e);
            return null;
        }
    }

    public <T> T getDataFromCache(String key, String hashKey, Class<T> clazz) {
        Object result = redisTemplate.opsForHash().get(key, hashKey);
        if (result == null) {
            return null;
        }
        return objectMapper.convertValue(result, clazz);
    }

    private void processMatchRemoval(MatchData matchData, String matchId, String matchKey) {
        if (matchData != null) {
            log.info("Match {} removed by {}", matchId, matchKey);
            //matchDataRepository.deleteByMatchId(Long.valueOf(matchId));
            redisTemplate.opsForHash().delete(RedisNameSpace.MATCH_DATA.getName(), matchKey);
            publishService.publishDataToGame(matchData, RedisNameSpace.MATCH_UPDATE.getName(), OperationType.REMOVE);
            marketCountService.removeMarketCountField("MATCH", String.valueOf(matchData.getSportId()) + matchData.getEventId() + matchId);
        }
    }

    private void processEventRemoval(EventData eventData, String eventId, String eventKey) {
        if (eventData != null) {
            log.info("event removed: {}", eventData.getEventId());
            //eventDataRepository.deleteByEventId(Long.valueOf(eventId));
            redisTemplate.opsForHash().delete(RedisNameSpace.EVENT_DATA.getName(), eventKey);
            publishService.publishDataToGame(eventData, RedisNameSpace.EVENT_UPDATE.getName(), OperationType.REMOVE);
            marketCountService.removeMarketCountField("EVENT", eventData.getSportId() + eventId);
        }
    }
}
