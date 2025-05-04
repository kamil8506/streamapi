package com.cassinus.streamapi.controller;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {
    @GetMapping("/streamapi/health")
    public String verifyRESTService() {
        return "RESTService Successfully started..";
    }
}