package com.cassinus.streamapi.service;

import com.cassinus.common.collection.SportData;
import com.cassinus.common.collection.Subscription;
import com.cassinus.common.enums.common.RedisNameSpace;
import com.cassinus.common.enums.common.Subscribe;
import com.cassinus.common.enums.market.FieldsEnum;
import com.cassinus.common.enums.messages.ConnectionStatus;
import com.cassinus.common.enums.messages.StatusCodeEnum;
import com.cassinus.common.exception.ConnectionException;
import com.cassinus.common.exception.StatusException;
import com.cassinus.common.model.market.MarketDataFilter;
import com.cassinus.common.model.market.MarketFilter;
import com.cassinus.common.model.message.StatusMessage;
import com.cassinus.common.repository.SubscriptionRepository;
import com.cassinus.streamapi.model.*;
import com.cassinus.streamapi.protocol.FutureResponse;
import com.cassinus.streamapi.protocol.RequestResponseProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

@Service
public class StartupService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${betfair.app-key}")
    private String appKey;
    @Value("${stream.host}")
    private String hostName;
    @Value("${stream.port}")
    private int port;
    @Value("${stream.timeout}")
    private int timeout;
    @Value("${stream.keepAlive}")
    private int keepAliveHeartBeat;
    @Value("${stream.conflateMs}")
    private long conflateMs;
    @Value("${stream.heartbeatMs}")
    private long heartbeatMs;

    @Value("${stream.autoReconnect}")
    private boolean autoReconnect;
    @Value("${stream.reconnectBackOff}")
    private long reconnectBackOff;


    private Socket client;
    private BufferedReader reader;
    private BufferedWriter writer;

    private static final String CRLF = "\r\n";
    private final RequestResponseProcessor processor;
    private ScheduledExecutorService keepAliveTimer;
    private int disconnectCounter;
    private int reconnectCounter;

    private volatile boolean isStarted;
    private volatile boolean isStopped;
    private long lastConnectTime;

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    private final SubscriptionRepository subscriptionRepository;

    @Autowired
    public StartupService(StreamCache streamCache, RedisTemplate<String, Object> redisTemplate,
                          ObjectMapper objectMapper, SubscriptionRepository subscriptionRepository) {
        this.processor = new RequestResponseProcessor(this::sendLineImpl, subscriptionRepository);
        this.processor.setChangeHandler(streamCache);
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.subscriptionRepository = subscriptionRepository;
    }

    @EventListener(ApplicationReadyEvent.class)
    private void startUp() {
        try {
            startStreamingClient(); // store redis in cache and use it after
            subscribeBySport();
        } catch (Exception ex) {
            logger.error("Error during startup: {}", ex.getMessage(), ex);
        }
    }

    private void startStreamingClient() throws StatusException, ConnectionException {
        disconnectCounter = 0;
        connectSocket();

        Thread thread = new Thread(this::run, "ESAClient");
        thread.start();

        keepAliveTimer = Executors.newSingleThreadScheduledExecutor();
        keepAliveTimer.scheduleAtFixedRate(() -> {
            try {
                keepAliveCheck();
            } catch (Exception e) {
                logger.error("KeepAlive check failed: {}", e.getMessage(), e);
            }
        }, timeout, timeout, TimeUnit.MILLISECONDS);

        connectAndAuthenticate();
        isStarted = true;
    }

    private void connectAndAuthenticate()
            throws ConnectionException, StatusException {
        try {
            ConnectionMessage result =
                    processor.getConnectionMessage().get(timeout, TimeUnit.MILLISECONDS);
            if (result == null) {
                throw new ConnectionException("No connection message");
            }
        } catch (Exception e) {
            throw new ConnectionException("Connection message failed", e);
        }
        authenticate();
    }


    private void run() {
        while (!isStopped) {
            //String line;
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    processor.receiveLine(line);
                }
            } catch (IOException e) {
                logger.error("ESAClient: Error received processing socket - disconnecting:", e);
                disconnected();
            }
        }
    }

    private void disconnected() {
        logger.info("disconnect count: {}", disconnectCounter++);
        if (isStarted && autoReconnect) {
            processor.disconnected();
            tryReconnect();
        } else {
            processor.stopped();
            isStopped = true;
        }
    }

    private void tryReconnect() {
        if (System.currentTimeMillis() - lastConnectTime < reconnectBackOff) {
            try {
                logger.info("Reconnect backoff for {}ms", reconnectBackOff);
                Thread.sleep(reconnectBackOff);
            } catch (InterruptedException e) {
                logger.error("Reconnect back off interrupted", e);
            }
        }

        try {
            connectSocket();
            keepAliveTimer.schedule(
                    this::connectAndAuthenticateAndResubscribe, 0, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Reconnect attempt={} failed: ", reconnectCounter, e);
            reconnectCounter++;
        }
    }

    private MarketSubscriptionMessage getMarketSubscriptionMessage(List<String> listOfSportId, MarketSubscriptionMessage marketSubscriptionMessage
            , boolean isMarketDataAvailable) {
        if (isMarketDataAvailable) {
            Optional<Subscription> subscription = subscriptionRepository.findByKey(Subscribe.SUBSCRIBE.getKey());
            subscription.ifPresent(sub -> {
                String initialClk = sub.getInitialClk();
                String clk = sub.getClk();

                if (initialClk != null && !initialClk.isBlank()
                        && clk != null && !clk.isBlank()) {
                    marketSubscriptionMessage.setInitialClk(initialClk);
                    marketSubscriptionMessage.setClk(clk);
                }
            });
        }

        MarketFilter marketFilter = new MarketFilter()
                .eventTypeIds(listOfSportId);

        MarketDataFilter marketDataFilter = new MarketDataFilter()
                .ladderLevels(3)
                .fields(Arrays.asList(FieldsEnum.EX_BEST_OFFERS_DISP, FieldsEnum.EX_MARKET_DEF, FieldsEnum.EX_TRADED_VOL));

        return marketSubscriptionMessage
                .marketFilter(marketFilter)
                .segmentationEnabled(true)
                .conflateMs(conflateMs)
                .heartbeatMs(heartbeatMs)
                .marketDataFilter(marketDataFilter);
    }

    public void marketSubscription(MarketSubscriptionMessage message)
            throws ConnectionException, StatusException {
        waitFor(processor.marketSubscription(message));
    }

    public void keepAliveCheck() {
        try {
            if (processor.getStatus() == ConnectionStatus.SUBSCRIBED) {
                // connection looks up
                if (processor.getLastRequestTime() + keepAliveHeartBeat
                        < System.currentTimeMillis()) {
                    // send a heartbeat to server to keep networks open
                    logger.info(
                            "Last Request Time is longer than {}: Sending Keep Alive Heartbate",
                            keepAliveHeartBeat);
                    heartbeat();
                } else if (processor.getLastResponseTime() + timeout < System.currentTimeMillis()) {
                    logger.info(
                            "Last Response Time is longer than {}: Sending Keep Alive Heartbate",
                            timeout);
                    heartbeat();
                }
                logger.info("keep alive not called");
            }
        } catch (Exception e) {
            logger.error("Keep alive failed", e);
        }
    }


    public void heartbeat() throws ConnectionException, StatusException {
        waitFor(processor.heartbeat(new HeartbeatMessage()));
    }

    private FutureResponse<StatusMessage> waitFor(FutureResponse<StatusMessage> task)
            throws StatusException, ConnectionException {
        StatusMessage statusMessage;
        try {
            statusMessage = task.get(timeout, TimeUnit.MILLISECONDS);
            if (statusMessage != null) {
                // server responed
                if (statusMessage.getStatusCode() == StatusCodeEnum.SUCCESS) {
                    return task;
                } else {
                    // status error
                    throw new StatusException(statusMessage);
                }
            } else {
                throw new ConnectionException("Result was expected");
            }

        } catch (InterruptedException e) {
            throw new ConnectionException("Future failed:", e);
        } catch (ExecutionException e) {
            throw new ConnectionException("Future failed:", e.getCause());
        } catch (CancellationException e) {
            throw new ConnectionException("Connection failed", e);
        } catch (TimeoutException e) {
            throw new ConnectionException("Future failed: (timeout)", e);
        }
    }

    private void connectSocket() throws ConnectionException {
        lastConnectTime = System.currentTimeMillis();

        try {
            disconnect();

            logger.info("ESAClient: Opening socket to: {}:{}", hostName, port);
            client = createSocket(hostName, port);
            client.setReceiveBufferSize(1024 * 1000 * 2); // shaves about 20s off firehose image.
            client.setSoTimeout(timeout);

            reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
            writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
        } catch (IOException e) {
            throw new ConnectionException("Failed to connect", e);
        }
    }

    public void disconnect() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                logger.warn("Unable to close socket", e);
            }
            client = null;
        }
    }

    private Socket createSocket(String hostName, int port) throws IOException {
        if (port == 443) {
            SocketFactory factory = SSLSocketFactory.getDefault();
            SSLSocket newSocket = (SSLSocket) factory.createSocket(hostName, port);
            newSocket.startHandshake();
            return newSocket;
        } else {
            return new Socket(hostName, port);
        }
    }

    private void subscribeBySport() throws ConnectionException, StatusException {
        List<String> listOfSportId = getSportIdsFromRedis();

        boolean isMarketDataAvailable = isMarketDataAvailable(RedisNameSpace.MARKET_DATA.getName());

        MarketSubscriptionMessage marketSubscriptionMessage
                = getMarketSubscriptionMessage(listOfSportId, new MarketSubscriptionMessage(), isMarketDataAvailable);

        logger.info("market subscription: {}", marketSubscriptionMessage);

        marketSubscription(marketSubscriptionMessage);
    }

    private void connectAndAuthenticateAndResubscribe() {
        try {

            // Connect and auth
            connectAndAuthenticate();

            // Resub markets
            MarketSubscriptionMessage marketSubscription = processor.getMarketResubscribeMessage();
            logger.info("market subscription: {}", marketSubscription);
            List<String> listOfSportId = getSportIdsFromRedis();
            marketSubscription = getMarketSubscriptionMessage(listOfSportId, marketSubscription, false);

            logger.info("Resubscribe to market subscription.");
            marketSubscription(marketSubscription);

            // Reset counter
            reconnectCounter = 0;
        } catch (Exception e) {
            logger.error("Reconnect failed {}, reconnectCounter: {}", e, reconnectCounter);
            reconnectCounter++;
        }
    }

    public List<String> getSportIdsFromRedis() {
        Map<Object, Object> sportDataMap = redisTemplate.opsForHash()
                .entries(RedisNameSpace.SPORT_DATA.getName());

        if (sportDataMap.isEmpty()) {
            return Collections.emptyList();
        }
        logger.info("sportdataMap: {}", sportDataMap);

        return sportDataMap.values().stream()
                .filter(Objects::nonNull)
                .map(value -> objectMapper.convertValue(value, SportData.class))
                .filter(sport -> sport.getStatus() == 1)
                .map(SportData::getSportId)
                .map(String::valueOf)
                .toList();
    }

    private void authenticate()
            throws ConnectionException, StatusException {
        AuthenticationMessage authenticationMessage = new AuthenticationMessage();
        authenticationMessage.setAppKey(appKey);
        authenticationMessage.setSession(AuthService.getSessionToken());
        waitFor(processor.authenticate(authenticationMessage));
    }

    public boolean isMarketDataAvailable(String hashKey) {
        Long size = redisTemplate.opsForHash().size(hashKey);
        return size > 0;
    }

    private void sendLineImpl(String line) throws ConnectionException {
        try {
            writer.write(line);
            writer.write(CRLF);
            writer.flush();
        } catch (IOException e) {
            logger.error("Error sending to socket - disconnecting", e);
            disconnect();
            throw new ConnectionException("Error sending to socket", e);
        }
    }
}