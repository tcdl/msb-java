package io.github.tcdl.msb.acceptance;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.ResponderOptions;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RequesterResponderTest {

    private MsbTestHelper helper = MsbTestHelper.getInstance();
    Config config = ConfigFactory.load();

    private static final String RESPONSE_BODY = "hello requester";
    private static final String REQUEST_BODY = "hello responder";
    private static final String NAMESPACE = "test:requester-responder";
    private static final int TIMEOUT_MS = 5000;

    @After
    public void tearDown() throws Exception {
        helper.shutdownAll();
    }

    @Test
    public void simpleRequestResponse() throws Exception {

        MsbContext responderContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));
        responderContext.getObjectFactory().createResponderServer(NAMESPACE, ResponderOptions.DEFAULTS, (req, ctx) -> {
            ctx.getResponder().send(RESPONSE_BODY);
        }, String.class)
                .listen();

        MsbContext requesterContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));

        int timeoutMs = 5000;
        CountDownLatch latch = new CountDownLatch(1);
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withWaitForResponses(1)
                .withResponseTimeout(timeoutMs)
                .build();

        requesterContext.getObjectFactory()
                .createRequester(NAMESPACE, requestOptions, String.class)
                .onResponse((resp, ctx) -> {
                    if (RESPONSE_BODY.equals(resp)) {
                        latch.countDown();
                    }
                })
                .publish(REQUEST_BODY);

        assertTrue("Expected response not received in time", latch.await(timeoutMs, TimeUnit.MILLISECONDS));
    }

    @Test
    public void tagsAreSentWithTheMessage() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        String tag = "CUSTOM_TAG";

        MsbContext responderContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));
        responderContext.getObjectFactory().createResponderServer(NAMESPACE, ResponderOptions.DEFAULTS,
                (req, ctx) -> {
                    if (ctx.getOriginalMessage().getTags().contains(tag)) {
                        latch.countDown();
                    }
                }, String.class)
                .listen();

        MsbContext requesterContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));

        RequestOptions requestOptions = new RequestOptions.Builder()
                .withWaitForResponses(1)
                .withResponseTimeout(TIMEOUT_MS)
                .build();

        requesterContext.getObjectFactory()
                .createRequesterForFireAndForget(NAMESPACE, requestOptions)
                .publish(REQUEST_BODY, tag);

        assertTrue("Message with the expected tag not received in time", latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    public void singleRedeliveryOnRetry() throws Exception {

        MsbContext requesterContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));
        MsbContext responderContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));

        int expectedDeliveryCount = 2;

        AtomicInteger deliveryCount = new AtomicInteger(0);
        responderContext.getObjectFactory().createResponderServer(NAMESPACE, ResponderOptions.DEFAULTS,
                (request, ctx) -> {
                    deliveryCount.incrementAndGet();
                    ctx.getAcknowledgementHandler().retryMessage();
                },
                String.class)
                .listen();

        requesterContext.getObjectFactory().createRequesterForFireAndForget(NAMESPACE).publish(REQUEST_BODY);

        //wait a bit
        TimeUnit.SECONDS.sleep(1);
        assertEquals("Incorrect delivery count", expectedDeliveryCount, deliveryCount.get());
    }

    @Test
    public void responseAfterRedelivery() throws Exception {

        MsbContext requesterContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));
        MsbContext responderContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));

        AtomicInteger deliveryCount = new AtomicInteger(0);
        responderContext.getObjectFactory().createResponderServer(NAMESPACE, ResponderOptions.DEFAULTS,
                (request, ctx) -> {
                    deliveryCount.incrementAndGet();
                    if (deliveryCount.get() > 1) {
                        ctx.getResponder().send(RESPONSE_BODY);
                    } else {
                        ctx.getAcknowledgementHandler().retryMessage();
                    }

                },
                String.class)
                .listen();

        CompletableFuture<String> deferredResponse = requesterContext.getObjectFactory()
                .createRequesterForSingleResponse(NAMESPACE, String.class)
                .request(REQUEST_BODY);

        String response = deferredResponse.get(1, TimeUnit.SECONDS);
        assertEquals("Unexpected response body", RESPONSE_BODY, response);
    }

    @Test
    public void responseFromDifferentThread() throws Exception {

        MsbContext requesterContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));
        MsbContext responderContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));

        responderContext.getObjectFactory().createResponderServer(NAMESPACE, ResponderOptions.DEFAULTS,
                (request, ctx) -> new Thread(() -> ctx.getResponder().send(RESPONSE_BODY)).start(),
                String.class)
                .listen();

        CompletableFuture<String> deferredResponse = requesterContext.getObjectFactory()
                .createRequesterForSingleResponse(NAMESPACE, String.class)
                .request(REQUEST_BODY);

        String response = deferredResponse.get(1, TimeUnit.SECONDS);
        assertEquals("Unexpected response body", RESPONSE_BODY, response);
    }

    @Test
    public void rejectedMessageNotRedelivered() throws Exception {

        MsbContext requesterContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));
        MsbContext responderContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));

        int expectedDeliveryCount = 1;

        AtomicInteger deliveryCount = new AtomicInteger(0);
        responderContext.getObjectFactory().createResponderServer(NAMESPACE, ResponderOptions.DEFAULTS,
                (request, ctx) -> {
                    deliveryCount.incrementAndGet();
                    ctx.getAcknowledgementHandler().rejectMessage();
                },
                String.class)
                .listen();

        requesterContext.getObjectFactory().createRequesterForFireAndForget(NAMESPACE).publish(REQUEST_BODY);

        //wait a bit
        TimeUnit.SECONDS.sleep(1);
        assertEquals("Incorrect delivery count", expectedDeliveryCount, deliveryCount.get());
    }

    @Test
    public void longRunningOnResponseHandler() throws Exception {

        int responsesCount = 10;
        MsbContext responderContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));
        responderContext.getObjectFactory().createResponderServer(NAMESPACE, ResponderOptions.DEFAULTS, (req, ctx) -> {
            for (int i = 0; i < responsesCount; i++) {
                ctx.getResponder().send(RESPONSE_BODY);
            }
        }, String.class)
                .listen();

        MsbContext requesterContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));

        int timeoutMs = 5000;
        CountDownLatch latch = new CountDownLatch(responsesCount);

        RequestOptions requestOptions = new RequestOptions.Builder()
                .withWaitForResponses(10)
                .withResponseTimeout(timeoutMs)
                .build();

        int delay = (timeoutMs / responsesCount) * 2;
        requesterContext.getObjectFactory()
                .createRequester(NAMESPACE, requestOptions, String.class)
                .onResponse((resp, ctx) -> {
                    //slow response handler
                    try {
                        TimeUnit.MILLISECONDS.sleep(delay);
                    } catch (InterruptedException irrelevant) {
                        //do nothing
                    }
                    latch.countDown();
                })
                .publish(REQUEST_BODY);

        double latencyFactor = 1.5;
        int delayInMicroseconds = delay * 1000;
        assertTrue("Not all responses processed in time", latch.await((int) (delayInMicroseconds * responsesCount * latencyFactor), TimeUnit.MICROSECONDS));
    }

    @Test
    public void messageWithForwardNamespace() throws Exception {

        String forwardNamespace = "test:forward";
        MsbContext responderContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));

        CountDownLatch latch = new CountDownLatch(1);
        responderContext.getObjectFactory().createResponderServer(NAMESPACE, ResponderOptions.DEFAULTS,
                (req, ctx) -> {
                    String receivedForwardNamespace = ctx.getOriginalMessage().getTopics().getForward();
                    if (forwardNamespace.equals(receivedForwardNamespace)) {
                        latch.countDown();
                    }
                }, String.class)
                .listen();

        MsbContext requesterContext = helper.initDistinctContext(MsbTestHelper.temporaryInfrastructure(config));

        RequestOptions requestOptions = new RequestOptions.Builder().withForwardNamespace(forwardNamespace).build();
        requesterContext.getObjectFactory()
                .createRequesterForFireAndForget(NAMESPACE, requestOptions)
                .publish(REQUEST_BODY);

        assertTrue("Message with expected forward topic not received in time", latch.await(1, TimeUnit.SECONDS));
    }
}