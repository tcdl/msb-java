package tcdl.msb.examples;

import tcdl.msb.Responder;
import tcdl.msb.config.MsbMessageOptions;
import tcdl.msb.messages.payload.RequestPayload;
import tcdl.msb.messages.payload.ResponsePayload;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rdro on 5/18/2015.
 */
public class ResponderExample {

    public static void main(String... args) {

        MsbMessageOptions options = new MsbMessageOptions();
        options.setNamespace("test:simple-requester");

        RequestPayload requestPayload = new RequestPayload();
        Map<String, String> headers = new HashMap<>();
        headers.put("From", "user@example.com");
        requestPayload.withHeaders(headers);

        Responder.createServer(options)
                .use(((request, response) -> {
                    System.out.print(">>> REQUEST: " + request.getHeaders());

                    ResponsePayload responsePayload = new ResponsePayload();
                    Map<String, String> body = new HashMap<>();
                    body.put("result", "response from responder");
                    responsePayload.withBody(body);

                    response.getResponder().send(responsePayload, null);
                }))
                .listen();
    }
}
