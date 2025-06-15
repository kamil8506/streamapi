package com.cassinus.streamapi.service;

import com.cassinus.common.enums.common.ApiHeaders;
import com.cassinus.common.model.common.BetfairLoginResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class AuthService {
    Logger logger = LoggerFactory.getLogger(AuthService.class);

    @Value("${betfair.type}")
    private String type;

    @Value("${betfair.app-key}")
    private String appKey;

    @Value("${betfair.username}")
    private String accUsername;

    @Value("${betfair.password}")
    private String accPassword;

    @Value("${betfair.cert-path}")
    private String certPath;

    @Value("${betfair.cert-password}")
    private String certPassword;

    @Value("${betfair.login-url}")
    private String loginUrl;

    @Value("${betfair.logout-url}")
    private String logoutUrl;

    @Value("${betfair.keepAlive-url}")
    private String keepAliveUrl;

    @Getter
    private String sessionToken;

    private CloseableHttpClient httpClient;

    private final RestTemplate restTemplate;
    private final RedisTemplate<String, Object> redisTemplate;

//    @Autowired
//    public AuthService(RestTemplate restTemplate) {
//        this.restTemplate = restTemplate;
//       // this.redisTemplate = redisTemplate;
//    }

    @PostConstruct
    public void init() {
        performLogin();
    }

    @Scheduled(
            initialDelayString = "${betfair.keepAlive-interval}",
            fixedDelayString = "${betfair.keepAlive-interval}"
    )
    //11 hours once
    public void keepAlive() {
        keepLoginAlive();
    }

    @PreDestroy
    public void destroy() {
        performLogout();
    }

    private void performLogin() {
        try {
            httpClient = getHttpClient();
            HttpPost httpPost = getHttpPost();

            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                org.apache.http.HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String responseString = EntityUtils.toString(entity);
                    BetfairLoginResponse betfairLoginResponse = parseResponse(responseString);
                    String loginStatus = betfairLoginResponse.loginStatus();
                    if ("SUCCESS".equals(loginStatus)) {
                        setSessionToken(betfairLoginResponse.sessionToken());
                        logger.info("Betfair login successful. Session token: {}, Login status: {}", sessionToken, loginStatus);
                    } else {
                        logger.error("Betfair login error: {}", betfairLoginResponse);
                    }
                    EntityUtils.consume(entity);
                }
            } catch (Exception e) {
                logger.error("Error during Betfair login", e);
            }
        } catch (Exception e) {
            logger.error("Error during Betfair login", e);
        } finally {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    logger.error("Error closing HttpClient", e);
                }
            }
        }
    }

    private void performLogout() {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set(ApiHeaders.ACCEPT.getHeaderName(), MediaType.APPLICATION_JSON_VALUE);
            headers.set(ApiHeaders.X_AUTHENTICATION.getHeaderName(), sessionToken);
            headers.set(ApiHeaders.X_APPLICATION.getHeaderName(), appKey);

            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<String> response = restTemplate.exchange(logoutUrl, HttpMethod.GET, entity, String.class);

            String responseString = response.getBody();
            logger.info("Logout response: {}", responseString);
            redisTemplate.delete("betfairToken");
        } catch (Exception e) {
            logger.error("Error calling logout API", e);
        }
    }

    private void keepLoginAlive() {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set(ApiHeaders.ACCEPT.getHeaderName(), MediaType.APPLICATION_JSON_VALUE);
            headers.set(ApiHeaders.X_AUTHENTICATION.getHeaderName(), sessionToken);
            headers.set(ApiHeaders.X_APPLICATION.getHeaderName(), appKey);

            HttpEntity<String> entity = new HttpEntity<>(headers);
            ResponseEntity<String> response = restTemplate.exchange(keepAliveUrl, HttpMethod.GET, entity, String.class);

            String responseString = response.getBody();
            logger.info("KeepAlive response: {}", responseString);
        } catch (Exception e) {
            logger.error("Error calling KeepAlive API", e);
        }
    }

    private void setSessionToken(String token) {
        sessionToken = token;
        redisTemplate.opsForValue().set("betfairToken", token);
    }

    private BetfairLoginResponse parseResponse(String responseString) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(responseString, BetfairLoginResponse.class);
    }

    private CloseableHttpClient getHttpClient() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(getKeyManagers(type, certPath, certPassword), null, new SecureRandom());
        SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, new DefaultHostnameVerifier());
        return HttpClients.custom().setSSLSocketFactory(sslSocketFactory).build();
    }

    private HttpPost getHttpPost() throws UnsupportedEncodingException {
        HttpPost httpPost = new HttpPost(loginUrl);
        List<NameValuePair> nameValuePairs = new ArrayList<>();
        nameValuePairs.add(new BasicNameValuePair(ApiHeaders.USERNAME.getHeaderName(), accUsername));
        nameValuePairs.add(new BasicNameValuePair(ApiHeaders.PASSWORD.getHeaderName(), accPassword));
        httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs));
        httpPost.setHeader(ApiHeaders.X_APPLICATION.getHeaderName(), appKey);
        return httpPost;
    }

    private KeyManager[] getKeyManagers(String type, String certPath, String certPassword) throws Exception {
        KeyStore keyStore = KeyStore.getInstance(type);
        keyStore.load(new FileInputStream(certPath), certPassword.toCharArray());
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, certPassword.toCharArray());
        return kmf.getKeyManagers();
    }
}