package us.juggl.twentysixteen.august;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.logging.log4j.Logger;
import org.asynchttpclient.*;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketTextListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;

import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/**
 * Performs a number of operations using various threads and executors for processing a WebSocket stream
 */
public class MultiThreaded {

    private static final ConcurrentLinkedDeque<JsonObject> jsonQueue = new ConcurrentLinkedDeque<>() ;
    private static final Gson gson = (new GsonBuilder()).create();
    private static final Logger LOG = org.apache.logging.log4j.LogManager.getLogger(MultiThreaded.class);
    private static final ConcurrentHashMap<String, LongAdder> eventCounts = new ConcurrentHashMap<>();
    private static final LongAdder totalStreamEvents = new LongAdder();
    private static final ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
    public static final int POLL_INTERVAL = 5;
    private static final ForkJoinPool WORKERS = new ForkJoinPool(3);

    /**
     * Application entry point
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.out.print("Enter your Meetup.com API key (https://secure.meetup.com/meetup_api/key/): ");
        Scanner input = new Scanner(System.in);
        String apiKey = input.nextLine();
        if (apiKey.matches("[0-9a-fA-F]{30}")) {
            System.out.print("\033[H\033[2J");
            System.out.flush();
            launchQueueSource(apiKey);
        } else {
            int keyLen = apiKey.length();
            LOG.error("The provided API key does not match the required format");
            LOG.info("The API key must be a 30 character hexidecimal string: "+keyLen);
            LOG.info("Usage: java -cp target/collections-streams-<version>.jar us.juggl.twentysixteen.august.MultiThreaded <API Key>");
        }
    }

    /**
     * Configure and launch a WebSocket client which adds data to concurrent data structures
     * @throws Exception
     */
    private static void launchQueueSource(String apiKey) throws Exception {
        AsyncHttpClientConfig cfg = new DefaultAsyncHttpClientConfig
                                            .Builder()
                                            .setConnectionTtl(10000)
                                            .setKeepAlive(true)
                                            .setKeepAliveStrategy(new DefaultKeepAliveStrategy())
                                            .build();
        AsyncHttpClient client = new DefaultAsyncHttpClient(cfg);
        WebSocket ws = client
                        .prepareGet("ws://stream.meetup.com/2/rsvps?sign=true&key="+apiKey)
                        .execute(new WebSocketUpgradeHandler.Builder().addWebSocketListener(new WebSocketTextListener() {
                            @Override
                            public void onMessage(String message) {
                                totalStreamEvents.increment();
                                jsonQueue.add(gson.fromJson(message, JsonObject.class));
                            }

                            @Override
                            public void onOpen(WebSocket websocket) {
                                LOG.info("Connected to WebSocket");
                                launchQueueViewer();
                            }

                            @Override
                            public void onClose(WebSocket websocket) {
                                LOG.info("Websocket closed");
                                sched.shutdown();
                            }

                            @Override
                            public void onError(Throwable t) {
                                LOG.error("WebSocket error");
                                sched.shutdown();
                            }
                        }).build()).get();
    }

    /**
     * Start a scheduled {@link Executor} to poll the {@link ConcurrentHashMap} and show the top 20 RSVPs for recent
     * MeetUp events.
     */
    private static void launchQueueViewer() {
        Runnable poller = new Runnable() {
            @Override
            public void run() {
                System.out.print("\033[H\033[2J");
                System.out.flush();
                String title = String.format("Top 20 recent meetup RSVPs (out of %d stream events)", totalStreamEvents.longValue());
                String sub = String.format("* Tracking %d distinct events", eventCounts.size());
                System.out.println(title);
                System.out.println(sub);
                final LongAdder lineNumbers = new LongAdder();
                eventCounts
                        .entrySet()
                        .stream()
                        .sorted((p, n) -> Long.compare(n.getValue().longValue(), p.getValue().longValue()))
                        .limit(20)
                        .forEach(e -> {
                            lineNumbers.increment();
                            String out = String.format("%2d. %10d %s", lineNumbers.intValue(), e.getValue().longValue(), e.getKey());
                            System.out.println(out);
                        });
            }
        };

        Runnable processor = new Runnable() {
            @Override
            public void run() {
                if (jsonQueue.size()>0) {                 // Check to see if there is any data waiting.
                    WORKERS.submit(() -> {  // Create a limited thread pool to run the parallel stream in
                        jsonQueue
                                .parallelStream()
                                .forEach(j -> {
                                    String event = j.getAsJsonObject("event").get("event_name").getAsString();
                                    eventCounts.putIfAbsent(event, new LongAdder());
                                    eventCounts.get(event).increment();
                                    jsonQueue.remove(j);
                                });
                    });
                }
            }
        };

        sched.scheduleAtFixedRate(processor, 1, 500, TimeUnit.MILLISECONDS);
        sched.scheduleAtFixedRate(poller, 1, POLL_INTERVAL, TimeUnit.SECONDS);
    }
}
