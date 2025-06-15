package com.cassinus.streamapi.service;

import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
@Slf4j
public class MarketProducer {

    private static final String TOPIC = "settlement-market";
    private final KafkaTemplate<String, String> kafkaTemplate;

    public MarketProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMarketStatus(String marketId, String status, long timestamp) {
        try {
            JSONObject kafkaMessage = new JSONObject();
            kafkaMessage.put("marketId", marketId);
            kafkaMessage.put("status", status);
            kafkaMessage.put("timestamp", timestamp);

            kafkaTemplate.send(TOPIC, marketId, kafkaMessage.toString())
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Kafka produce success for marketId: {}", marketId);
                        } else {
                            log.error("Kafka produce failed for marketId: {}, reason: {}", marketId, ex.getMessage());
                        }
                    });

        } catch (Exception e) {
            log.error("Error constructing Kafka message for marketId: {}, reason: {}", marketId, e.getMessage());
        }
    }
}
