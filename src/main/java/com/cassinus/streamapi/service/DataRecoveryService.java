package com.cassinus.streamapi.service;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.cassinus.common.collection.MarketData;
import com.cassinus.common.collection.MarketSettledResult;
import com.cassinus.common.collection.RunnerData;
import com.cassinus.common.enums.common.OperationType;
import com.cassinus.common.enums.market.MarketStatus;
import com.cassinus.streamapi.market.Market;
import com.cassinus.streamapi.market.MarketCache;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import com.cassinus.common.enums.common.ApiHeaders;
import com.cassinus.common.enums.common.RedisNameSpace;
import com.cassinus.common.enums.market.PriceProjection;
import com.cassinus.common.exception.FeederException;
import com.cassinus.common.model.common.APINGExceptionResponse;
import com.cassinus.common.repository.MarketDataRepository;
import com.cassinus.common.repository.MarketResultRepository;
import com.cassinus.common.service.SequenceGeneratorService;
import com.cassinus.common.util.LevelPriceSize;
import com.cassinus.common.util.PriceSize;
import com.cassinus.streamapi.model.BetfairErrorResponse;
import com.cassinus.streamapi.model.Filter;
import com.cassinus.streamapi.model.MarketBookResult;
import com.cassinus.streamapi.model.Runner;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;

@Service
@RequiredArgsConstructor
@Slf4j
public class DataRecoveryService {
	@Value("${api.rest-url}")
	private String restUrl;
	@Value("${api.app-key}")
	private String appKey;
	@Value(("${market.max-count}"))
	private String marketCount;

	private final RedisTemplate<String, Object> redisTemplate;
	private final ObjectMapper objectMapper;
	private final WebClient.Builder webClientBuilder;
	private final MarketDataRepository marketDataRepository;
	private final PublishService publishService;
	private final AuthService authService;
	private final MarketProducer producer;
	private final MarketCache marketCache;
	private final MarketResultRepository marketResultRepository;
	private final SequenceGeneratorService sequenceGeneratorService;


	
	@Async
	public void dataRecovery() {
		Map<Object, Object> marketDataFromRedis = redisTemplate.opsForHash().entries(RedisNameSpace.MARKET_DATA.getName());

		Set<String> marketId = marketDataFromRedis.keySet().stream().map(Object::toString).collect(Collectors.toSet());
		List<MarketBookResult> marketBookList = fetchMarketBookDetailFromMarketList(marketId).collectList().block();

		compareMarketBookResult(marketDataFromRedis,marketBookList);
		
	}
	
	private void compareMarketBookResult(Map<Object, Object> marketDataFromRedis, List<MarketBookResult> marketBookList) {
			marketBookList.stream().forEach(marketBook->{
				boolean isCLosedMarket=false;
				MarketData currentRedisData = objectMapper.convertValue(marketDataFromRedis.get(marketBook.getMarketId()), MarketData.class);
				if(marketBook.getStatus().equalsIgnoreCase(MarketStatus.CLOSED.getValue())) {
					isCLosedMarket = true;
					updatePublishDBRedisData(currentRedisData,marketBook,isCLosedMarket);
				}else {
					isCLosedMarket = false;
					currentRedisData = updateOddsValues(currentRedisData,marketBook);
					updatePublishDBRedisData(currentRedisData,marketBook,isCLosedMarket);					
				}
			});
			
			
		
	}

	private void updatePublishDBRedisData(MarketData marketData, MarketBookResult marketBook , boolean isCLosedMarket) {
		if(isCLosedMarket){
			publishService.publishDataToGame(marketData, RedisNameSpace.MARKET_UPDATE.getName(), OperationType.REMOVE);  // publish to game service

			redisTemplate.opsForHash().delete(RedisNameSpace.MARKET_DATA.getName(), marketBook.getMarketId()); // delete marketdata from redis

			marketDataRepository.updateMarketStatus(marketBook.getMarketId(), "CLOSED"); // update from db
 
			redisTemplate.opsForHash().delete(RedisNameSpace.MARKET.getName(), marketBook.getMarketId()); // delete market from redis
			
			marketCache.decrementLiveAndTotalMatchCount(marketData.getMarketId(),marketData.getMatchId(),marketData.getSportId()); // decrement live and match count
			marketCache.decrementMarketCount(marketData, (marketData.getSportId() == 7 || marketData.getSportId() == 4339));  //decrement market count
			boolean flag = insertValuesToMarketResult(marketData,marketBook);
			if(flag)
				producer.sendMarketStatus(marketBook.getMarketId(), "CLOSED", System.currentTimeMillis()); //Writing in kafka producer
			
		} else {
			  redisTemplate.opsForHash().put(RedisNameSpace.MARKET_DATA.getName(), marketData.getMarketId(), marketData); //update marketData in redis
	          publishService.publishDataToGame(marketData, RedisNameSpace.MARKET_UPDATE.getName(), OperationType.UPDATE); //Publish to game service
	          marketDataRepository.save(marketData); //update in db
	          
	       // update market cache and db
//	            redisTemplate.opsForHash().put(RedisNameSpace.MARKET.getName(), marketData.getMarketId(), market); //Commented this as we don't have market change data
		}
	}

	private Flux<MarketBookResult> fetchMarketBookDetailFromMarketList(Set<String> marketId) {
		Filter filterObj = Filter.builder().marketIds(marketId.stream().toList())
				.priceProjection(Map.of("priceData",List.of(PriceProjection.EX_BEST_OFFERS)))
				.eventTypeIds(Collections.emptyList())
				.eventIds(Collections.emptyList())
				.competitionIds(Collections.emptyList())
				.eventIds(Collections.emptyList())
				.marketTypeCodes(Collections.emptyList())
				.marketCountries(Collections.emptyList())
				.selectionId("")
				.marketId("")				
				.build();
		String filterStr = filterObj.toJson();
		HttpClient httpClient = HttpClient.create().keepAlive(false);
		ReactorClientHttpConnector httpConnector = new ReactorClientHttpConnector(httpClient);

		WebClient webClient = webClientBuilder.baseUrl(restUrl)
				.clientConnector(httpConnector)
				.defaultHeader(ApiHeaders.X_APPLICATION.getHeaderName(), appKey)
				.defaultHeader(ApiHeaders.ACCEPT.getHeaderName(), MediaType.APPLICATION_JSON_VALUE)
				.build();

		return webClient.post()
        .uri("listMarketBook/")
        .contentType(MediaType.APPLICATION_JSON)
        .header(ApiHeaders.X_AUTHENTICATION.getHeaderName(), authService.getSessionToken())
        .bodyValue(filterStr)
        .retrieve()
        .bodyToFlux(MarketBookResult.class)
        .onErrorResume(WebClientResponseException.class, this::handleWebClientResponseException);

	}

	private <T> Flux<T> handleWebClientResponseException(WebClientResponseException ex) {
		try {
			String responseBody = ex.getResponseBodyAsString();
			log.error("WebClientResponseException: Status code: {}, Response body: {}", ex.getStatusCode(), responseBody);

			ObjectMapper mapper = new ObjectMapper();
			BetfairErrorResponse error = mapper.readValue(ex.getResponseBodyAsString(), BetfairErrorResponse.class);

			if (error.getFaultcode() != null || error.getFaultstring() != null || error.getDetail() != null) {
				APINGExceptionResponse apiError = error.getDetail().getApingException();
				return Flux.error(apiError != null
						? new FeederException(apiError.getErrorDetails(), apiError.getErrorCode(), apiError.getRequestUUID())
						: new FeederException(error.toString(), error.getFaultstring(), null));
			}
		} catch (Exception e) {
			log.error("Failed to parse error: {}", e.getMessage(), e);
		}
		return Flux.error(ex);
	}

	private MarketData updateOddsValues(MarketData currentRedisData, MarketBookResult marketBook){
		 if (Boolean.FALSE.equals(currentRedisData.isInPlay()) && Boolean.TRUE.equals(marketBook.isInplay())) {
			 	marketCache.processInplayChanges(currentRedisData.getSportId(), currentRedisData.getEventId(), currentRedisData.getMatchId());
			 	marketCache.incrementLiveMatchCount(currentRedisData.getMarketId(), currentRedisData.getMatchId(),currentRedisData.getSportId(),currentRedisData.getCustomerIds());
		 }
		currentRedisData.setStatus(marketBook.getStatus());
		currentRedisData.setInPlay(marketBook.isInplay());
		currentRedisData.setUpdatedTime(LocalDateTime.now());
		Map<Long,Runner> map = marketBook.getRunners().stream().collect(Collectors.toMap(r-> r.getSelectionId(), r->r));
		for(RunnerData runnerData : currentRedisData.getRunners()) {
			List<LevelPriceSize> prizeBackList = convertToLevelPriceSizes( map.get(runnerData.getRunnerId()).getEx().availableToBack());
			List<LevelPriceSize> prizeLayList = convertToLevelPriceSizes(map.get(runnerData.getRunnerId()).getEx().availableToLay());
			runnerData.setAvailableToBack(prizeBackList);
			runnerData.setAvailableToLay(prizeLayList);
		}
		
		return currentRedisData;
		
	}
	
	 private boolean insertValuesToMarketResult(MarketData marketData, MarketBookResult marketBook) {
		 	boolean response = false;
	        Map<Long,String> runnerMap = new LinkedHashMap<>();
	        for (RunnerData runner : marketData.getRunners()) {
	            runnerMap.putIfAbsent(runner.getRunnerId(), runner.getRunnerName());
	        }

	        for(Runner runnerData : marketBook.getRunners()) {
	            if(runnerData.getStatus().equals("WINNER")){ //Only add winner data
	            	response = true;
	                List<MarketSettledResult> marketResultList = marketResultRepository.findAllByMarketIdAndIsActive(marketData.getMarketId(),true);
	                if(marketResultList != null && !marketResultList.isEmpty()) { // if data already present we are making resettle as true and active as false
	                    for(MarketSettledResult marketResult : marketResultList) {
	                        marketResult.setResettle(true);
	                        marketResult.setActive(false);
	                        marketResultRepository.save(marketResult);
	                    }
	                    MarketSettledResult marketResult = new MarketSettledResult(sequenceGeneratorService.generateSequence(MarketSettledResult.SEQUENCE_NAME, 0L),
	                    		marketData.getMarketId(),runnerData.getSelectionId(),runnerMap.get(runnerData.getSelectionId()),false,true,
	                            "StreamAPI",LocalDateTime.now());
	                    marketResultRepository.save(marketResult);
	                }else {
	                    MarketSettledResult marketResult = new MarketSettledResult(sequenceGeneratorService.generateSequence(MarketSettledResult.SEQUENCE_NAME, 0L),
	                    		marketData.getMarketId(),runnerData.getSelectionId(),runnerMap.get(runnerData.getSelectionId()),false,true,
	                            "StreamAPI",LocalDateTime.now());
	                    marketResultRepository.save(marketResult);
	                }
	               
	            }
	        }
	        return response;
	    }
	 
	 private List<LevelPriceSize> convertToLevelPriceSizes(List<PriceSize> priceSizes) {
	        return IntStream.range(0, priceSizes.size())
	                .mapToObj(i -> {
	                    PriceSize ps = priceSizes.get(i);
	                    return new LevelPriceSize(i, ps.getPrice(), ps.getSize());
	                })
	                .toList();
	    }


}
