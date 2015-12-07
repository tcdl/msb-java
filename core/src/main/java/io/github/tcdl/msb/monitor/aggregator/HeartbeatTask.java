package io.github.tcdl.msb.monitor.aggregator;

import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.support.Utils;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends heartbeat requests and passes the aggregated responses to the registered handler. This task is meant to be scheduled periodically.
 */
public class HeartbeatTask implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatTask.class);

    private int heartbeatTimeoutMs;
    private ObjectFactory objectFactory;
    private Callback<List<Message>> heartbeatHandler;

    public HeartbeatTask(int heartbeatTimeoutMs, ObjectFactory objectFactory, Callback<List<Message>> heartbeatHandler) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.objectFactory = objectFactory;
        this.heartbeatHandler = heartbeatHandler;
    }

    @Override
    public void run() {
        try {
            LOG.debug("Sending heartbeat request...");
            RequestOptions requestOptions = new RequestOptions.Builder()
                    .withResponseTimeout(heartbeatTimeoutMs)
                    .withWaitForResponses(-1)
                    .build();

            RestPayload emptyPayload = new RestPayload.Builder().build();

            List<Message> messages = Collections.synchronizedList(new LinkedList<>());
            objectFactory.createRequester(Utils.TOPIC_HEARTBEAT, requestOptions)
                    .onRawResponse((message, achHandler) -> {messages.add(message);})
                    .onEnd(end -> heartbeatHandler.call(messages))
                    .publish(emptyPayload);

            LOG.debug("Heartbeat request sent");
        } catch (Exception e) {
            LOG.error("Error during heartbeat invocation", e);
        }
    }
}
