package io.github.tcdl.msb.acceptance;

import io.github.tcdl.msb.acceptance.payload.MyPayload;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.payload.RestPayload;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RequesterResponderTest {

    private static final Integer NUMBER_OF_RESPONSES = 1;

    final String NAMESPACE = "test:requester-responder-example";

    private MsbTestHelper helper = MsbTestHelper.getInstance();

    private CountDownLatch passedLatch;

    public boolean isPassed() {
        try {
            passedLatch.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }

        return passedLatch.getCount() == 0;
    }

    public void runRequesterResponder() throws Exception {
        helper.initDefault();
        // running responder server
        helper.createResponderServer(NAMESPACE, (request, responder) -> {
            System.out.println(">>> QUERY: " + request.getQuery().getQ());
            responder.sendAck(1000, NUMBER_OF_RESPONSES);
            helper.respond(responder);
        }, MyPayload.class)
        .listen();

        // sending a request
        Requester<RestPayload<Object, Object, Object, Map<String, Object>>> requester = helper.createRequester(NAMESPACE, NUMBER_OF_RESPONSES);
        passedLatch = new CountDownLatch(1);
        helper.sendRequest(requester, NUMBER_OF_RESPONSES, payload -> passedLatch.countDown());
    }
}
