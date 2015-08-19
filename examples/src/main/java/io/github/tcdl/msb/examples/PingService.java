package io.github.tcdl.msb.examples;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.payload.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PingService {
    private static final Logger LOG = LoggerFactory.getLogger(PingService.class);

    public static void main(String[] args) {
        MsbContext msbContext = new MsbContextBuilder().
                enableShutdownHook(true).
                build();

        MessageTemplate messageTemplate = new MessageTemplate().withTags("ping-service");
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withAckTimeout(1000)
                .withResponseTimeout(2000)
                .withWaitForResponses(1)
                .withMessageTemplate(messageTemplate)
                .build();

        ObjectFactory objectFactory = msbContext.getObjectFactory();
        Requester<Payload> requester = objectFactory.createRequester("pingpong:namespace", requestOptions)
                .onResponse(payload -> LOG.info(String.format("Received response '%s'", payload.getBody()))) // Handling the one response
                .onEnd(arg -> LOG.info("Received all expected responses")); // Handling all response arrival or timeout

        Payload pingPayload = new Payload.Builder<Object, Object, Object, String>()
                .withBody("PING")
                .build();

        requester.publish(pingPayload, UUID.randomUUID().toString()); // Send the message
    }
}
