package io.github.tcdl.examples;

import io.github.tcdl.Requester;

/**
 * Created by rdrozdov-tc on 6/15/15.
 */
public class RequesterResponderTest {

    private static final Integer NUMBER_OF_RESPONSES = 1;

    final String NAMESPACE = "test:requester-responder-example";

    private MSBUtil util = MSBUtil.getInstance();

    private boolean passed;

    public boolean isPassed() {
        return passed;
    }

    public void runRequesterResponder() throws Exception {
        // running responder server
        util.createResponderServer(NAMESPACE, (request, responder) -> {
            responder.sendAck(1000, NUMBER_OF_RESPONSES);
            util.respond(responder);
        })
        .listen();

        // sending a request
        Requester requester = util.createRequester(NAMESPACE, NUMBER_OF_RESPONSES);
        util.sendRequest(requester, NUMBER_OF_RESPONSES, payload -> passed = true);
    }
}
