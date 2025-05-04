package com.cassinus.streamapi.protocol;

import java.util.EventListener;

public interface ConnectionStatusListener extends EventListener {
    void connectionStatusChange(ConnectionStatusChangeEvent statusChangeEvent);
}
