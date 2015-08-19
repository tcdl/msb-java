package io.github.tcdl.msb.examples;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.payload.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PongService {
    private static final Logger LOG = LoggerFactory.getLogger(PongService.class);

    public static void main(String[] args) {
        MsbContext msbContext = new MsbContextBuilder().
                enableShutdownHook(true). // Instruct the builder to auto-register hook that does graceful shutdown
                build();

        ObjectFactory objectFactory = msbContext.getObjectFactory();
        MessageTemplate messageTemplate = new MessageTemplate().withTags("pong-static-tag");
        ResponderServer responderServer = objectFactory.createResponderServer("pingpong:namespace", messageTemplate, (request, responder) -> {
            // Response handling logic
            LOG.info(String.format("Handling %s...", request.getBody()));

            Payload pongPayload = new Payload.Builder<Object, Object, Object, String>()
                    .withBody("PONG")
                    .build();

            responder.send(pongPayload);

            LOG.info("Response sent");
        });
        responderServer.listen(); // Need not forget to hook up the responder server
    }
}
