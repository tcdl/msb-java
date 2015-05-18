package io.github.tcdl.examples;

import io.github.tcdl.Requester;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.TwoArgumentsAdapter;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.BasicPayload;
import io.github.tcdl.messages.payload.RequestPayload;

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

        RequestPayload requestPayload = new RequestPayload();
        Map<String, String> headers = new HashMap<>();
        headers.put("From", "user@example.com");
        requestPayload.withHeaders(headers);

        Requester requester = new Requester(options, null);

        requester.on(new Event("response"), new TwoArgumentsAdapter<BasicPayload, Message>() {
            @Override public void onEvent(BasicPayload payload, Message message) {
                System.out.println(">>> RESPONSE body: " + payload.getBody());
            }
        });

        requester.publish(requestPayload);
    }
}
