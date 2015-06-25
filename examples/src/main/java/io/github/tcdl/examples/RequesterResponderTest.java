package io.github.tcdl.examples;

import io.github.tcdl.Requester;

/**
 * Created by rdrozdov-tc on 6/15/15.
 */
public class RequesterResponderTest extends BaseExample {

    private static final Integer NUMBER_OF_RESPONSES = 1;

    final String NAMESPACE = "test:requester-responder-example";

    private boolean passed;

    public boolean isPassed() {
        return passed;
    }

    public void runRequesterResponder() throws Exception {
        // running responder server
        createResponderServer(NAMESPACE,(request, responder) -> {
                    responder.sendAck(1000, NUMBER_OF_RESPONSES);
                    respond(responder);
                })
                .listen();

        // sending a request
        Requester requester = createRequester(NAMESPACE, NUMBER_OF_RESPONSES);
        sendRequest(requester, "RequesterResponderTest:request", true, NUMBER_OF_RESPONSES, null, payload -> passed = true);
    }
}
