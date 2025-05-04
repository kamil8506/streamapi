package com.cassinus.streamapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.cassinus.streamapi", "com.cassinus.common"})
@EnableCaching
@EnableScheduling
public class StreamApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(StreamApiApplication.class, args);
    }
}