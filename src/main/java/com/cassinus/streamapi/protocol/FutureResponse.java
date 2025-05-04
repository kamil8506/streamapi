package com.cassinus.streamapi.protocol;

import java.util.concurrent.FutureTask;

public class FutureResponse<T> extends FutureTask<T> {
    private static final Runnable NULL = () -> {};

    public FutureResponse() {
        super(NULL, null);
    }

    public void setResponse(T response) {
        set(response);
    }

    @Override
    public void setException(Throwable t) {
        super.setException(t);
    }
}
