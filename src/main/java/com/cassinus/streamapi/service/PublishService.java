package com.cassinus.streamapi.service;

import com.cassinus.common.enums.common.OperationType;
import com.cassinus.common.exception.PublishDataException;
import com.cassinus.common.model.common.DataWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class PublishService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public PublishService(RedisTemplate<String, Object> redisTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    public <T> void publishDataToGame(T data, String topicName, OperationType operationType) {
        //System.out.println("Hello MK");
        try {
            DataWrapper<T> wrapper = new DataWrapper<>(data, operationType);
            String publishDataString = objectMapper.writeValueAsString(wrapper);
            //System.out.println("publishDataString" +publishDataString);
            redisTemplate.convertAndSend(topicName, publishDataString);
        } catch (Exception e) {
            throw new PublishDataException("Failed to publish data to topic: " + topicName, e, data);
        }
    }
}