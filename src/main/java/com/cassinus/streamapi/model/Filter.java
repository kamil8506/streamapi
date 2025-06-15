package com.cassinus.streamapi.model;

import java.util.List;
import java.util.Map;

import com.cassinus.common.enums.market.PriceProjection;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Filter {
	private List<String> eventTypeIds;
    private List<String> competitionIds;
    private List<String> eventIds;
    private List<String> marketIds;
    private List<String> marketTypeCodes;
    private List<String> marketCountries;
    private Map<String, List<PriceProjection>> priceProjection;
    private List<String> venues;
    private String selectionId;
    private String marketId;
    private boolean useFilter;
    private boolean useMarketProjection;
    private boolean isRacingSport;


    public String toJson() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            return "{}";
        }
    }

}
