package com.cassinus.streamapi.model;

import com.cassinus.common.model.common.APINGExceptionResponse;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class ErrorDetail {
    @JsonProperty("APINGException")
    private APINGExceptionResponse apingException;
    @JsonProperty("exceptionname")
    private String exceptionName;
}
