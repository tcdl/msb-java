package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.Requester;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rdro on 5/18/2015.
 */
public class RequesterExample {

    public static void main(String... args) {

        MsbContext msbContext = new MsbContext.MsbContextBuilder().build();
        MsbMessageOptions options = new MsbMessageOptions();
        options.setNamespace("test:simple-requester");
        options.setWaitForResponses(1);
        options.setAckTimeout(100);
        options.setResponseTimeout(2000);

        Map<String, String> headers = new HashMap<>();
        headers.put("From", "user@example.com");
        Payload requestPayload = new Payload.PayloadBuilder().setHeaders(headers).build();

        Requester requester = Requester.create(options, msbContext);

        requester
                .onAcknowledge(acknowledge ->
                    System.out.println(">>> ACK timeout: " + acknowledge.getTimeoutMs())
                )
                .onResponse(payload ->
                    System.out.println(">>> RESPONSE body: " + payload.getBody())
                );

        requester.publish(requestPayload);
    }
}
