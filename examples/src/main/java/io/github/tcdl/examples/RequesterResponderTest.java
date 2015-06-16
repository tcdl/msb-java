package io.github.tcdl.examples;

import io.github.tcdl.Requester;

/**
 * Created by rdrozdov-tc on 6/15/15.
 */
public class RequesterResponderTest extends BaseExample {

    final String NAMESPACE = "test:requester-responder-example";

    private boolean passed;

    public boolean isPassed() {
        return passed;
    }

    public void runRequesterResponder() throws Exception {
        // running responder server
        createResponderServer(NAMESPACE)
                .use(((request, responder) -> {
                    responder.sendAck(1000, 1);
                    respond(responder);
                }))
                .listen();

        // sending a request
        Requester requester = createRequester(NAMESPACE, 1);
        sendRequest(requester, "RequesterResponderTest:request", true, 1, null, payload -> passed = true);
    }
}
