package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.ResponderServer;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleResponderExample {
    public static void main(String... args) {
        MsbMessageOptions options = new MsbMessageOptions();

        if (args.length != 1) {
            System.out.println("If you would like set topic which will be used please pass it through parameter");
            System.out.println("Example:java SimpleResponderExample test:simple-queue");
            System.exit(1);
        } else {
            options.setNamespace(args[0]);
        }

        MsbContext msbContext = new MsbContext.MsbContextBuilder().build();

        ResponderServer.create(options, msbContext)
                .use(((request, responder) -> {
                    System.out.print(">>> REQUEST: " + request.getHeaders());

                    Thread.sleep(500);

                    responder.send(createResponse(options.getNamespace()));
                }))
                .listen();
    }

    private static Payload createResponse(String message) {
        Map<String, String> body = new HashMap<>();
        body.put("SimpleResponderExample", message);
        return new Payload.PayloadBuilder().setBody(body).build();
    }
}
