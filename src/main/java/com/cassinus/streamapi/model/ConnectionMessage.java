package com.cassinus.streamapi.model;

import com.cassinus.common.model.message.ResponseMessage;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class ConnectionMessage extends ResponseMessage {
    private String connectionId;

    public ConnectionMessage connectionId(String connectionId) {
        this.connectionId = connectionId;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectionMessage connectionMessage = (ConnectionMessage) o;
        return Objects.equals(this.connectionId, connectionMessage.connectionId) &&
                super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionId, super.hashCode());
    }

    @Override
    public String toString() {
        return "class ConnectionMessage {\n" +
                "    " + toIndentedString(super.toString()) + "\n" +
                "    connectionId: " + toIndentedString(connectionId) + "\n" +
                "}";
    }

    private String toIndentedString(Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }
}