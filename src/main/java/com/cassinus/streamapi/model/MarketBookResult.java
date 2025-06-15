package com.cassinus.streamapi.model;

import java.time.LocalDateTime;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MarketBookResult {
    private String marketId;
    private boolean marketDataDelayed;
    private String status;
    private int betDelay;
    private boolean bspReconciled;
    private boolean complete;
    private boolean inplay;
    private int numberOfWinners;
    private int numberOfRunners;
    private int numberOfActiveRunners;
    private LocalDateTime lastMatchTime;
    private double totalMatched;
    private double totalAvailable;
    private boolean crossMatching;
    private boolean runnersVoidable;
    private long version;
    private List<Runner> runners;
}