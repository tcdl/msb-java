package io.github.tcdl.msb.acceptance;

import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.Responder;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class RequesterResponderRunner { //todo clean refactor or throw away if same functionality is covered by the other tests

    private static final Integer NUMBER_OF_RESPONSES = 1;

    final String NAMESPACE = "test:requester-responder-example";

    private MsbTestHelper helper = MsbTestHelper.getInstance();

    private CountDownLatch passedLatch;

    @Test
    public void runTest() throws Exception {
        helper.initDefault();
        // running responder server
        helper.createResponderServer(NAMESPACE, (request, responderContext) -> {
            System.out.println(">>> REQUEST: " + request);
            Responder responder = responderContext.getResponder();
            responder.sendAck(1000, NUMBER_OF_RESPONSES);
            responder.send("Pong");
        }, String.class)
        .listen();

        // sending a request
        Requester<String> requester = helper.createRequester(NAMESPACE, NUMBER_OF_RESPONSES, String.class);
        passedLatch = new CountDownLatch(1);
        helper.sendRequest(requester, "Ping", true, NUMBER_OF_RESPONSES, arg -> {}, payload -> passedLatch.countDown());

        assertTrue(isPassed());
    }

    public boolean isPassed() {
        try {
            passedLatch.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }

        return passedLatch.getCount() == 0;
    }
}
