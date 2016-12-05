package io.github.tcdl.msb.acceptance;

import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.ResponderOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class BlockingRequesterTest {

    private static final String REQUEST_NAMESPACE = "test:future";
    private static final String REQUEST_BODY = "Hi there";
    private static final String RESPONSE_BODY = "Hello, Future!";

    private MsbContext msbContext;

    @Before
    public void setUp() throws Exception {
        msbContext = new MsbContextBuilder().build();
    }

    @Test
    public void singleFutureResponse() throws Exception {

        //set up responder
        msbContext.getObjectFactory()
                .createResponderServer(REQUEST_NAMESPACE, ResponderOptions.DEFAULTS, (request, responderContext) -> {
                    if (REQUEST_BODY.equals(request)) {
                        responderContext.getResponder().send(RESPONSE_BODY);
                    } else {
                        responderContext.getResponder().sendAck(0, 0);
                    }
                }, String.class)
                .listen();

        //prepare and send request
        int timeoutMs = 2000;
        RequestOptions requestOptions = new RequestOptions.Builder().withResponseTimeout(timeoutMs).build();

        CompletableFuture<String> futureResponse = msbContext.getObjectFactory()
                .createRequesterForSingleResponse(REQUEST_NAMESPACE, String.class, requestOptions)
                .request(REQUEST_BODY);

        //wait for response
        try {
            String actualResponseBody = futureResponse.get(timeoutMs, TimeUnit.MILLISECONDS);
            assertEquals("Unexpected response body", RESPONSE_BODY, actualResponseBody);
        } catch (Exception e) {
            fail("Response was not received in time: " + e);
        }
    }

    @Test
    public void singleFutureResponseAfterAcknowledge() throws Exception {

        int newTimeout = 500;
        int initialTimeout = 200;

        //set up responder
        msbContext.getObjectFactory().createResponderServer(REQUEST_NAMESPACE, ResponderOptions.DEFAULTS, (request, responderContext) -> {
            //send ack requesting additional time for response generation, wait, send response
            responderContext.getResponder().sendAck(newTimeout, 1);
            TimeUnit.MILLISECONDS.sleep(newTimeout / 2);
            responderContext.getResponder().send(RESPONSE_BODY);
        }, String.class)
                .listen();

        //prepare and send request
        RequestOptions requestOptions = new RequestOptions.Builder().withResponseTimeout(initialTimeout).build();
        CompletableFuture<String> futureResponse = msbContext.getObjectFactory()
                .createRequesterForSingleResponse(REQUEST_NAMESPACE, String.class, requestOptions)
                .request(REQUEST_BODY);

        //wait for response
        try {
            String actualResponseBody = futureResponse.get(newTimeout, TimeUnit.MILLISECONDS);
            assertEquals("Unexpected response body", RESPONSE_BODY, actualResponseBody);
        } catch (Exception e) {
            fail("Response was not received in time: " + e);
        }
    }

    @Test(timeout = 5000, expected = CancellationException.class)
    public void tooManyRemainingResponses() throws Exception {

        //set up responder
        msbContext.getObjectFactory().createResponderServer(REQUEST_NAMESPACE, ResponderOptions.DEFAULTS, (request, responderContext) -> {
            //send ack requesting additional time for response generation, wait, send response
            responderContext.getResponder().sendAck(null, 2); //oops
            responderContext.getResponder().send(RESPONSE_BODY);
        }, String.class)
                .listen();

        //prepare and send request
        CompletableFuture<String> futureResponse = msbContext.getObjectFactory()
                .createRequesterForSingleResponse(REQUEST_NAMESPACE, String.class, RequestOptions.DEFAULTS)
                .request(REQUEST_BODY);

        //wait for response
        futureResponse.get();
    }

    @After
    public void tearDown() throws Exception {
        msbContext.shutdown();
    }
}
