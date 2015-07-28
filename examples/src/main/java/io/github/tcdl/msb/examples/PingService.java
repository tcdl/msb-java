package io.github.tcdl.msb.examples;

import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.payload.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PingService {
    private static final Logger LOG = LoggerFactory.getLogger(PingService.class);

    public static void main(String[] args) {
        MsbContext msbContext = new MsbContextBuilder().
                enableShutdownHook(true).
                build();

        RequestOptions requestOptions = new RequestOptions.Builder()
                .withAckTimeout(1000)
                .withResponseTimeout(2000)
                .withWaitForResponses(1)
                .build();

        ObjectFactory objectFactory = msbContext.getObjectFactory();
        Requester requester = objectFactory.createRequester("pingpong:namespace", requestOptions)
                .onResponse(payload -> LOG.info(String.format("Received response '%s'", payload.getBody()))) // Handling the one response
                .onEnd(arg -> LOG.info("Received all expected responses")); // Handling all response arrival or timeout

        Payload pingPayload = new Payload.Builder()
                .withBody("PING")
                .build();

        requester.publish(pingPayload); // Send the message
    }
}
