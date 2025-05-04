package com.cassinus.streamapi.protocol;


import com.cassinus.common.exception.ConnectionException;

public interface RequestSender {

    void sendLine(String line) throws ConnectionException;
}
