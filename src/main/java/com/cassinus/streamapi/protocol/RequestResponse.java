package com.cassinus.streamapi.protocol;

import com.cassinus.common.enums.messages.StatusCodeEnum;
import com.cassinus.common.model.message.RequestMessage;
import com.cassinus.common.model.message.StatusMessage;
import lombok.Getter;

import java.util.function.Consumer;

@Getter
public class RequestResponse {
    private final FutureResponse<StatusMessage> future = new FutureResponse<>();
    private final RequestMessage request;
    private final Consumer<RequestResponse> onSuccess;
    private final int id;

    public RequestResponse(int id, RequestMessage request, Consumer<RequestResponse> onSuccess) {
        this.id = id;
        this.request = request;
        this.onSuccess = onSuccess;
    }

    public void processStatusMessage(StatusMessage statusMessage) {
        if (statusMessage.getStatusCode() == StatusCodeEnum.SUCCESS) {
            if (onSuccess != null) onSuccess.accept(this);
            future.setResponse(statusMessage);
        }
    }

    public void setException(Exception e) {
        future.setException(e);
    }
}
