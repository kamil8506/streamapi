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
import jakarta.annotation.PreDestroy;
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
    @Value("${stream.isRecoveryMode}")
    private boolean isRecoveryMode;


    private Socket client;
    private BufferedReader reader;
    private BufferedWriter writer;

    private static final String CRLF = "\r\n";
    private final RequestResponseProcessor processor;
    //private ScheduledExecutorService keepAliveTimer;
    private int disconnectCounter;

    private volatile boolean isStarted;
    private volatile boolean isStopped;

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;

    private final SubscriptionRepository subscriptionRepository;
    private final Object reconnectLock = new Object();
    private volatile boolean reconnecting = false;
    //private final ScheduledExecutorService reconnectExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService conExecutor = Executors.newScheduledThreadPool(2);
    private volatile boolean maintenanceMode = false;

    private volatile Thread readerThread;
    
    private final DataRecoveryService dataRecoveryService;

    @Autowired
    public StartupService(StreamCache streamCache, RedisTemplate<String, Object> redisTemplate,
                          ObjectMapper objectMapper, SubscriptionRepository subscriptionRepository
                          ,DataRecoveryService dataRecoveryService) {
        this.processor = new RequestResponseProcessor(this::sendLineImpl, subscriptionRepository);
        this.processor.setChangeHandler(streamCache);
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.subscriptionRepository = subscriptionRepository;
        this.dataRecoveryService = dataRecoveryService;
    }

    @Autowired
    private AuthService authService;

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
//      To recover past market data from betfair api and update in redis & db
        if(isRecoveryMode)
        	dataRecoveryService.dataRecovery();
        startReaderThread();
        conExecutor.scheduleAtFixedRate(() -> {
            try {
                keepAliveCheck();
            } catch (Exception e) {
                logger.error("KeepAlive check failed: {}", e.getMessage(), e);
            }
        }, timeout, timeout, TimeUnit.MILLISECONDS);

        connectAndAuthenticate();
        isStarted = true;
    }

    private void startReaderThread() {
        stopReaderThread();
        readerThread = new Thread(this::run, "ESAClient");
        readerThread.start();
        //logger.info("Reader thread started.");
    }

//    private void stopReaderThread() {
//        if (readerThread != null) {
//            readerThread.interrupt();
//            readerThread = null;
//            //logger.info("Reader thread interrupted.");
//        }
//    }

    private synchronized void stopReaderThread() {
        if (readerThread != null) {
            readerThread.interrupt(); // mark interrupted
            try {
                if (reader != null) reader.close(); // force unblock readLine
                if (client != null) client.close(); // ensures underlying socket closed
            } catch (IOException e) {
                logger.warn("Error while closing reader/client in stopReaderThread: {}", e.getMessage());
            }
            readerThread = null;
        }
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
        try {
            while (!isStopped && !Thread.currentThread().isInterrupted()) {
                String line;
                while (client != null && !client.isClosed() && (line = reader.readLine()) != null) {
                    processor.receiveLine(line);

                    // Interrupt flag check (optional, for extra safety)
                    if (Thread.currentThread().isInterrupted()) {
                        logger.info("Reader thread interrupted, exiting inner loop.");
                        break;
                    }
                }
                // If it has reached this point, either the client has closed the connection, readLine returned null, or an exception occurred
                logger.warn("Socket read loop ended, disconnecting...");
                disconnected();
            }
        } catch (IOException e) {
            logger.error("ESAClient read error:", e);
            disconnected();
        }
        logger.info("Reader thread exiting cleanly.");
    }

    private void disconnected() {
        logger.warn("Disconnected. Attempt: {}", disconnectCounter++);
        processor.disconnected();

        if (isStarted && autoReconnect) {
            tryReconnect();
        } else {
            isStopped = true;
            processor.stopped();
        }
    }

    private void tryReconnect() {
        disconnect();
        synchronized (reconnectLock) {
            if (reconnecting) {
                logger.warn("Reconnect already in progress, skipping...");
                return;
            }
            reconnecting = true;
        }
        conExecutor.execute(() -> {
            int maxAttempts = 120;
            long reconnectStart = System.currentTimeMillis();
            long maxReconnectDuration = TimeUnit.HOURS.toMillis(1); // 1 hour max

            try {
                while (!isStopped && autoReconnect) {
                    int attempt = 0;
                    long backoff = reconnectBackOff;

                    while (attempt < maxAttempts && !isStopped && autoReconnect) {
                        if (Thread.currentThread().isInterrupted()) {
                            logger.warn("Reconnect thread interrupted before attempt.");
                            return;
                        }

                        try {
                            attempt++;
                            logger.info("Reconnect attempt #{}", attempt);

                            processor.disconnected(); // clear pending futures

                            connectSocket();
                            dataRecoveryService.dataRecovery(); // Proceeding for data recovery only after sucessful socket connection
                            startReaderThread();
                            connectAndAuthenticateAndResubscribe();

                            long duration = System.currentTimeMillis() - reconnectStart;
                            logger.info("Reconnection successful on attempt #{} after {}ms", attempt, duration);
                            onReconnectSuccess();
                            return;
                        } catch (Exception ex) {
                            logger.warn("Reconnect attempt #{} failed: {}", attempt, ex.getMessage());
                            backoff = calculateBackoffWithJitter(attempt);
                            logger.info("Backoff for {}ms before next attempt...", backoff);
                            try {
                                TimeUnit.MILLISECONDS.sleep(backoff);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                logger.warn("Reconnect sleep interrupted.");
                                return;
                            }
                        }
                    }
                    // fail-safe limit after N failures
                    if (System.currentTimeMillis() - reconnectStart > maxReconnectDuration) {
                        logger.error("Reconnect failed after {} attempts. Cooling down for 1 minute...", attempt);
                        sleepQuietly(60000);
                        attempt = 0; // reset attempts after cool down
                        reconnectStart = System.currentTimeMillis(); // reset timer
                    }
                }

            } finally {
                synchronized (reconnectLock) {
                    reconnecting = false;
                }
            }
        });
    }

    private void sleepQuietly(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private long calculateBackoffWithJitter(int attempt) {
        long base = Math.min((1L << attempt) * reconnectBackOff, TimeUnit.MINUTES.toMillis(2));
        long jitter = ThreadLocalRandom.current().nextLong(500, 1000);
        return base + jitter;
    }

    private void onReconnectSuccess() {
        logger.info("Reconnect successful. Metrics or alert can be triggered here.");
    }

    private void onReconnectFailure() {
        logger.error("Reconnect permanently failed. Trigger alert or metrics.");
    }

    public void setMaintenanceMode(boolean status) {
        this.maintenanceMode = status;
        if (!status && !reconnecting && autoReconnect) {
            tryReconnect(); // resume after maintenance
        }
    }

    @PreDestroy
    public void onDestroy() {
        logger.info("Shutting down stream client");
        shutdown();
    }

    public void shutdown() {
        isStopped = true;
        disconnect();
        conExecutor.shutdownNow();
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

    private void heartbeat() throws ConnectionException, StatusException {
        waitFor(processor.heartbeat(new HeartbeatMessage()));
    }

    public void keepAliveCheck() {
        if (processor.getStatus() != ConnectionStatus.SUBSCRIBED) {
            logger.debug("Skipping keep-alive: Not in SUBSCRIBED state");
            return;
        }

        long currentTime = System.currentTimeMillis();
        boolean needHeartbeat = processor.getLastRequestTime() + keepAliveHeartBeat < currentTime
                || processor.getLastResponseTime() + timeout < currentTime;

        if (needHeartbeat) {
            logger.info("Sending keep-alive heartbeat...");
            try {
                heartbeat();
            } catch (Exception e) {
                logger.error("KeepAlive heartbeat failed: {}", e.getMessage(), e);
                tryReconnect();
            }
        }
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
        long lastConnectTime = System.currentTimeMillis();

        try {
            disconnect();

            logger.info("ESAClient: Opening socket to: {}:{}", hostName, port);
            client = createSocket(hostName, port);
            client.setReceiveBufferSize(1024 * 1000 * 2); // shaves about 20s off firehose image.
            client.setSoTimeout(timeout);

            reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
            writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
            //Compare existing redis data and get new status from betfair
        } catch (IOException e) {
            throw new ConnectionException("Failed to connect", e);
        }
    }

    public void disconnect() {
        stopReaderThread();
        try {
            if (reader != null) reader.close();
            if (writer != null) writer.close();
            if (client != null) client.close();
        } catch (IOException e) {
            logger.warn("Unable to disconnect socket ", e);
        } finally {
           reader = null;
           writer = null;
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
            int reconnectCounter = 0;
        } catch (Exception e) {
//            logger.error("Reconnect failed {}, reconnectCounter: {}", e, reconnectCounter);
//            reconnectCounter++;
            logger.error("Reconnect + resubscribe failed", e);
            disconnected();
        }
    }

    public List<String> getSportIdsFromRedis() {
        Map<Object, Object> sportDataMap = redisTemplate.opsForHash()
                .entries(RedisNameSpace.SPORT_DATA.getName());

        if (sportDataMap.isEmpty()) {
            return Collections.emptyList();
        }
        //logger.info("sportdataMap: {}", sportDataMap);

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
        // Fetch sessionToken using the injected AuthService
        String sessionToken = authService.getSessionToken();
        if (sessionToken == null || sessionToken.isEmpty()) {
            throw new ConnectionException("Session token is missing or invalid.");
        }
        authenticationMessage.setSession(sessionToken);
        waitFor(processor.authenticate(authenticationMessage));
    }

    public boolean isMarketDataAvailable(String hashKey) {
        Long size = redisTemplate.opsForHash().size(hashKey);
        return size > 0;
    }

    private boolean isConnected() {
        return writer != null && client != null && !client.isClosed();
    }

    private void sendLineImpl(String line) throws ConnectionException {
        try {
            if (!isConnected()) {
                logger.error("Not connected. Cannot send line: {}", line);
                throw new ConnectionException("Not connected to socket");
            }
            writer.write(line);
            writer.write(CRLF);
            writer.flush();
        } catch (IOException e) {
            logger.error("Error sending to socket - disconnecting", e);
            disconnected();
            throw new ConnectionException("Error sending to socket", e);
        }
    }
}