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
    private boolean passed;

    private MsbContext msbContext;
    private MsbMessageOptions options = new MsbMessageOptions();

    SimpleRequesterExample(MsbContext msbContext, String namespace) {
        this.msbContext = msbContext;
        options.setNamespace(namespace);
    }

    public boolean isPassed() {
        return passed;
    }

    public void setPassed(boolean passed) {
        this.passed = passed;
    }

    public MsbContext getMsbContext() {
        return msbContext;
    }

    public void setMsbContext(MsbContext msbContext) {
        this.msbContext = msbContext;
    }

    @Test
    public void runSimpleRequesterExample() {
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
                                passed = true;
                            } catch (Throwable throwable) {
                                passed = false;
                            }
                        }
                );

        requester.publish(requestPayload);
    }
}
