package io.github.tcdl.examples;

import io.github.tcdl.Requester;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.TwoArgumentsAdapter;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rdro on 5/18/2015.
 */
public class RequesterExample {

    public static void main(String... args) {

        MsbMessageOptions options = new MsbMessageOptions();
        options.setNamespace("test:simple-requester");
        options.setWaitForResponses(1);
        options.setAckTimeout(5000);
        options.setResponseTimeout(10000);

        Map<String, String> headers = new HashMap<>();
        headers.put("From", "user@example.com");
        Payload requestPayload = new Payload.PayloadBuilder().setHeaders(headers).build();

        Requester requester = new Requester(options, null);

        requester.on(new Event("response"), new TwoArgumentsAdapter<Payload, Message>() {
            @Override public void onEvent(Payload payload, Message message) {
                System.out.println(">>> RESPONSE body: " + payload.getBody());
            }
        });

        requester.publish(requestPayload);
    }
}
