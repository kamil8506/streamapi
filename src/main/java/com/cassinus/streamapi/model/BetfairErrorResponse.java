package com.cassinus.streamapi.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class BetfairErrorResponse {
    private String faultcode;
    private String faultstring;
    private ErrorDetail detail;
}