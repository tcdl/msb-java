package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.Requester;
import io.github.tcdl.ResponderServer;
import org.junit.Test;

/**
 * Created by rdrozdov-tc on 6/15/15.
 */
public class RequesterResponder extends BaseAcceptanceTest {

    final String NAMESPACE = "test:requester-responder-example";

    @Test
    public void run() throws Exception {

        MsbContext msbContext = createContext();

        // running responder server
        createResponderServer(msbContext, NAMESPACE)
                .use(((request, responder) -> {
                    responder.sendAck(1000, 1);
                    respond(responder);
                }))
                .listen();

        // sending a request
        Requester requester = createRequester(msbContext, NAMESPACE, 1);
        sendRequest(requester, true, 1);
    }
}
