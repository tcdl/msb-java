package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.Requester;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Created by anstr on 6/9/2015.
 */
public class SimpleRequesterExample {
    @Test
    public static void main(String... args) {

        MsbMessageOptions options = new MsbMessageOptions();

        if (args.length != 1) {
            System.out.println("If you would like set topic which will be used please pass it through parameter");
            System.out.println("Example:java SimpleRequesterExample test:simple-queue");
            System.exit(1);
        } else {
            options.setNamespace(args[0]);
        }

        MsbContext msbContext = new MsbContext.MsbContextBuilder().build();
        options.setWaitForResponses(1);
        options.setResponseTimeout(10000);

        Map<String, String> headers = new HashMap<>();
        headers.put("From", "user@example.com");
        Payload requestPayload = new Payload.PayloadBuilder().setHeaders(headers).build();

        Requester requester = Requester.create(options, msbContext);

        requester
                .onResponse(payload -> {
                            System.out.println(">>> RESPONSE body: " + payload.getBody());
                            System.out.println();
                            try {
                                assertTrue(payload.getBody().toString().contains("test:simple-queue2"));
                                assertTrue(payload.getBody().toString().contains("test:simple-queue3"));
                            }catch (Throwable throwable) {
                                System.out.println("!!!!!!!!!!Test wasn't pass!!!!!!!!!!");
                                System.exit(1);
                            }
                            System.out.println("Test Passed Successfully");
                            System.exit(0);
                        }
                );

        requester.publish(requestPayload);
    }
}
