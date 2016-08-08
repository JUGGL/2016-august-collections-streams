package us.juggl.twentysixteen.august;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.logging.log4j.Logger;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.Response;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketTextListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;

import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Performs a number of operations using various threads and executors for processing a WebSocket stream
 */
public class MultiThreaded {

    private static final ConcurrentLinkedDeque<JsonObject> jsonQueue = new ConcurrentLinkedDeque<>() ;
    private static final Gson gson = (new GsonBuilder()).create();
    private static final Logger LOG = org.apache.logging.log4j.LogManager.getLogger(MultiThreaded.class);
    private static final ConcurrentHashMap<String, LongAdder> eventCounts = new ConcurrentHashMap<>();
    private static final LongAdder totalStreamEvents = new LongAdder();
    public static final int POLL_INTERVAL = 5;

    /**
     * Application entry point
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        launchQueueSource();
        launchQueueProcessor();
    }

    private static void launchQueueSource() throws Exception {
        AsyncHttpClient client = new DefaultAsyncHttpClient();
        WebSocket ws = client
                        .prepareGet("ws://stream.meetup.com/2/rsvps?sign=true&key=3c4e124459174351555854296d7163")
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
                            }

                            @Override
                            public void onError(Throwable t) {
                                LOG.error("WebSocket error");
                            }
                        }).build()).get();
    }

    private static void launchQueueProcessor() {
        while (1==1) {
            jsonQueue
                .parallelStream()
                .forEach(j -> {
                    String event = j.getAsJsonObject("event").get("event_name").getAsString();
                    if (!eventCounts.containsKey(event)) eventCounts.put(event, new LongAdder());
                    eventCounts.get(event).increment();
                    jsonQueue.remove(j);
                });
        }
    }

    private static void launchQueueViewer() {
        Runnable poller = new Runnable() {
            @Override
            public void run() {
                System.out.print("\033[H\033[2J");
                System.out.flush();
                System.out.println("Top 20 recent meetup RSVPs (out of "+totalStreamEvents.longValue()+" events)");
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
        ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
        sched.scheduleAtFixedRate(poller, 1, POLL_INTERVAL, TimeUnit.SECONDS);
    }
}
