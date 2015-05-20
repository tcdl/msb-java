package io.github.tcdl.adapters;

import io.github.tcdl.exception.ChannelException;

import java.util.Queue;
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

    @Override
    public void publish(String jsonMessage) throws ChannelException {
        log.info("Sending request {}", jsonMessage);
        requests.offer(jsonMessage);
        log.info("Requests to process {}", requests);
    }

    @Override
    public void subscribe(RawMessageHandler messageHandler) {
        log.info("Subscribed. Process first message from list {}", requests);
        this.messageHandler = messageHandler;
        if (messageHandler != null && !requests.isEmpty()) {
            String jsonMessage = requests.peek();
            handleMessage(jsonMessage);
        }
    }

    @Override
    public void unsubscribe() {
        messageHandler = null;
    }

    private void handleMessage(String jsonMessage) {
        log.debug("Retrieved response {}", jsonMessage);
        messageHandler.onMessage(jsonMessage);
    }
    
    public void clearAllMessages () {
        requests.clear();
    }
}
