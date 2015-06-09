package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.Requester;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleRequesterExample {
    public static void main(String[] args) {

        MsbMessageOptions options = new MsbMessageOptions();

        if (args.length == 1) {
            System.out.println("If you would like set topic which will be used please pass it through parameter");
            System.out.println("Example:java RequesterExample test:simple-queue");
            options.setNamespace(args[0]);
        } else {
            options.setNamespace("test:simple-queue");
        }

        MsbContext msbContext = new MsbContext.MsbContextBuilder().build();
        options.setWaitForResponses(1);
        options.setResponseTimeout(10000);

        Map<String, String> headers = new HashMap<>();
        headers.put("From", "user@example.com");
        Payload requestPayload = new Payload.PayloadBuilder().setHeaders(headers).build();

        Requester requester = Requester.create(options, msbContext);

        requester
                .onResponse(payload ->
                                System.out.println(">>> RESPONSE body: " + payload.getBody())
                );

        requester.publish(requestPayload);
    }
}
