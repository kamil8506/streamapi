package com.cassinus.streamapi.config;

import com.cassinus.common.util.RunnerId;
import com.cassinus.common.util.RunnerIdKeyDeserializer;
import com.cassinus.common.util.RunnerIdKeySerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addKeySerializer(RunnerId.class, new RunnerIdKeySerializer());
        module.addKeyDeserializer(RunnerId.class, new RunnerIdKeyDeserializer());
        mapper.registerModule(module);
        mapper.registerModule(new JavaTimeModule()); // Register JavaTimeModule separately
        return mapper;
    }
}
   