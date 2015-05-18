package io.github.tcdl.adapters;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rdro on 4/24/2015.
 */
public class MockAdapter implements Adapter {

    public static Logger log = LoggerFactory.getLogger(MockAdapter.class);

    private static MockAdapter instance = new MockAdapter();

    private Queue<String> requests = new LinkedBlockingDeque<String>();
    private RawMessageHandler messageHandler;

    private MockAdapter() {
    }

    public static MockAdapter getInstance() {
        return instance;
    }

    public void publish(String jsonMessage) {
        requests.offer(jsonMessage);

        log.debug("Sending request {}", jsonMessage);

        System.out.println(jsonMessage);
    }

    @Override
    public void subscribe(RawMessageHandler messageHandler) {
        this.messageHandler = messageHandler;
        if (messageHandler != null && !requests.isEmpty()) {
            String jsonMessage = requests.peek();
            handleMessage(jsonMessage);
        }
    }

    public void consume(String jsonMessage) {
        if (messageHandler != null) {
            log.debug("Retrieved response {}", jsonMessage);
            messageHandler.onMessage(jsonMessage);
        }
    }

    private void handleMessage(String jsonMessage) {
        CompletableFuture.supplyAsync(() -> {
            log.debug("Retrieved response {}", jsonMessage);
            messageHandler.onMessage(jsonMessage);
            return null;
        });
    }
}
