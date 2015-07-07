package io.github.tcdl.acceptance;

import io.github.tcdl.api.Requester;

/**
 * Created by rdrozdov-tc on 6/15/15.
 */
public class RequesterResponderTest {

    private static final Integer NUMBER_OF_RESPONSES = 1;

    final String NAMESPACE = "test:requester-responder-example";

    private MsbTestHelper helper = MsbTestHelper.getInstance();

    private boolean passed;

    public boolean isPassed() {
        return passed;
    }

    public void runRequesterResponder() throws Exception {
        helper.initDefault();
        // running responder server
        helper.createResponderServer(NAMESPACE, (request, responder) -> {
            responder.sendAck(1000, NUMBER_OF_RESPONSES);
            helper.respond(responder);
        })
        .listen();

        // sending a request
        Requester requester = helper.createRequester(NAMESPACE, NUMBER_OF_RESPONSES);
        helper.sendRequest(requester, NUMBER_OF_RESPONSES, payload -> passed = true);
    }
}
