package com.cassinus.streamapi.model;

import com.cassinus.common.util.PriceSize;
import java.util.List;

public record ExchangePrices(List<PriceSize> availableToBack, List<PriceSize> availableToLay,
                             List<PriceSize> tradedVolume) {

}