package com.cassinus.streamapi.protocol;

import com.cassinus.streamapi.model.ConnectionMessage;
import com.cassinus.streamapi.model.MarketChangeMessage;
import com.cassinus.common.model.order.OrderChangeMessage;
import com.cassinus.common.model.message.StatusMessage;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "op",
        visible = true)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConnectionMessage.class, name = "connection"),
        @JsonSubTypes.Type(value = StatusMessage.class, name = "status"),
        @JsonSubTypes.Type(value = MarketChangeMessage.class, name = "mcm"),
        @JsonSubTypes.Type(value = OrderChangeMessage.class, name = "ocm"),
})
interface MixInResponseMessage {}
