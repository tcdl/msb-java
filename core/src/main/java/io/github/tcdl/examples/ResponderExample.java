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

                    response.getResponder().sendAck(10000, 3, null);
                    Thread.sleep(5000);

                    response.getResponder().send(createResponse(1), null);
                    response.getResponder().send(createResponse(2), null);
                    response.getResponder().send(createResponse(3), null);
                }))
                .listen();
    }

    private static Payload createResponse(int seq) {
        Map<String, String> body = new HashMap<>();
        body.put("result", "response " + seq + " from responder");
        return  new Payload.PayloadBuilder().setBody(body).build();
    }
}
