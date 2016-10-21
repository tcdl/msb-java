package io.github.tcdl.msb.acceptance;

import com.google.common.base.Joiner;
import io.github.tcdl.msb.api.Requester;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;

public class MultipleRequesterResponder {

    private static final Integer NUMBER_OF_RESPONSES = 1;

    private MsbTestHelper util = MsbTestHelper.getInstance();

    private String responderNamespace;
    private String requesterNamespace1;
    private String requesterNamespace2;

    private final AtomicInteger responseCounter = new AtomicInteger();
    private final List<String> responseBodies = new CopyOnWriteArrayList<>();

    MultipleRequesterResponder(String responderNamespace, String requesterNamespace1, String requesterNamespace2) {
        this.responderNamespace = responderNamespace;
        this.requesterNamespace1 = requesterNamespace1;
        this.requesterNamespace2 = requesterNamespace2;
    }

    public void runMultipleRequesterResponder() {
        util.createResponderServer(responderNamespace, (request, responderContext) -> {
            System.out.print(">>> REQUEST: " + request);
            Runnable onFinalResponse = () -> {
                String responses = Joiner.on(StringUtils.EMPTY).join(responseBodies);
                responderContext.getResponder().send("response from MultipleRequesterResponder:" + responses);
            };
            createAndRunRequester(requesterNamespace1, onFinalResponse);
            createAndRunRequester(requesterNamespace2, onFinalResponse);
        }, String.class)
        .listen();
    }

    public void shutdown() {
        util.shutdown();
    }

    private void createAndRunRequester(String namespace, Runnable onFinalResponse) {
        Requester<String> requester = util.createRequester(namespace, NUMBER_OF_RESPONSES, null, 5000, String.class);
        try {
            util.sendRequest(requester, "PING", NUMBER_OF_RESPONSES, response -> {
                System.out.println(">>> RESPONSE body: " + response);
                responseBodies.add(response);
                if(responseCounter.incrementAndGet() == 2) {
                    onFinalResponse.run();
                }
            });
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
