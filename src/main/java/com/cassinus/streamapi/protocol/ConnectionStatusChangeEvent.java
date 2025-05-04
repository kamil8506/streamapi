package com.cassinus.streamapi.protocol;

import com.cassinus.common.enums.messages.ConnectionStatus;
import lombok.Getter;

import java.util.EventObject;

@Getter
public class ConnectionStatusChangeEvent extends EventObject {
    private final ConnectionStatus oldStatus;
    private final ConnectionStatus newStatus;

    public ConnectionStatusChangeEvent(
            Object source, ConnectionStatus oldStatus, ConnectionStatus newStatus) {
        super(source);
        this.oldStatus = oldStatus;
        this.newStatus = newStatus;
    }
}
