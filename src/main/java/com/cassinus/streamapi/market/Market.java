package com.cassinus.streamapi.market;

import com.cassinus.common.enums.market.MarketStatus;
import com.cassinus.common.model.market.*;
import com.cassinus.common.util.RunnerId;
import com.cassinus.common.util.Utils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Market {
    private static final Logger logger = LoggerFactory.getLogger(Market.class);
    @Id
    @Indexed(unique = true)
    private String marketId;
    private Map<RunnerId, MarketRunner> marketRunners = new ConcurrentHashMap<>();
    private MarketDefinition marketDefinition;
    private double tv;
    private MarketSnap snap;

    public Market(String marketId) {
        this.marketId = marketId;
    }

    public void onMarketChange(MarketChange marketChange) {
        boolean isImage = Boolean.TRUE.equals(marketChange.getImg());

        Optional.ofNullable(marketChange.getMarketDefinition())
                .ifPresent(this::onMarketDefinitionChange);

        Optional.ofNullable(marketChange.getRc())
                .ifPresent(l -> l.forEach(p -> onPriceChange(isImage, p)));

        tv = Utils.selectPrice(isImage, tv, marketChange.getTv());

        MarketSnap newSnap = new MarketSnap();
        newSnap.setMarketId(marketId);
        newSnap.setMarketDefinition(marketDefinition);
        newSnap.setMarketRunners(
                marketRunners.values().stream()
                        .map(MarketRunner::getSnap)
                        .toList());
        newSnap.setTradedVolume(tv);
        snap = newSnap;
    }

    private void onPriceChange(boolean isImage, RunnerChange runnerChange) {
        MarketRunner marketRunner =
                getOrAdd(new RunnerId(runnerChange.getId(), runnerChange.getHc()));
        marketRunner.onPriceChange(isImage, runnerChange);
    }

    private void onMarketDefinitionChange(MarketDefinition marketDefinition) {
        this.marketDefinition = marketDefinition;
        Optional.ofNullable(marketDefinition.getRunners())
                .ifPresent(rds -> rds.forEach(this::onRunnerDefinitionChange));
    }

    private void onRunnerDefinitionChange(RunnerDefinition runnerDefinition) {
        MarketRunner marketRunner =
                getOrAdd(new RunnerId(runnerDefinition.getId(), runnerDefinition.getHc()));
        marketRunner.onRunnerDefinitionChange(runnerDefinition);
    }

    private MarketRunner getOrAdd(RunnerId runnerId) {
        return marketRunners.computeIfAbsent(runnerId, MarketRunner::new);
    }

    public boolean isClosed() {
        return (marketDefinition != null
                && marketDefinition.getStatus() == MarketStatus.CLOSED);
    }
}