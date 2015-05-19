package io.github.tcdl.examples;

import io.github.tcdl.Responder;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rdro on 5/18/2015.
 */
public class ResponderExample {

    public static void main(String... args) {

        MsbMessageOptions options = new MsbMessageOptions();
        options.setNamespace("test:simple-requester");

        Responder.createServer(options)
                .use(((request, response) -> {
                    System.out.print(">>> REQUEST: " + request.getHeaders());

                    response.getResponder().sendAck(10000, 1, null);
                    Thread.sleep(5000);

                    Map<String, String> body = new HashMap<>();
                    body.put("result", "response from responder");
                    Payload responsePayload = new Payload.PayloadBuilder().setBody(body).build();

                    response.getResponder().send(responsePayload, null);
                }))
                .listen();
    }
}
