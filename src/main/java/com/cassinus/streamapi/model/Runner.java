package com.cassinus.streamapi.model;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class Runner {
    private long selectionId;
    private double handicap;
    private String status;
    private double totalMatched;
    private double lastTradedPrice;
    private ExchangePrices ex;
}