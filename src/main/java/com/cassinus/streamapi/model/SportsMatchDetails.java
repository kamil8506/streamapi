package com.cassinus.streamapi.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SportsMatchDetails {
    private List<Long> ids;
    private int count;
    private String partnerId;
    private Map<Long, Set<String>> marketAgainstMatch;
}
