package io.github.tcdl.msb.acceptance;

import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.payload.Payload;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MultipleRequesterResponder {

    private static final Integer NUMBER_OF_RESPONSES = 1;

    private MsbTestHelper util = MsbTestHelper.getInstance();

    private String responderNamespace;
    private String requesterNamespace1;
    private String requesterNamespace2;

    MultipleRequesterResponder(String responderNamespace, String requesterNamespace1, String requesterNamespace2) {
        this.responderNamespace = responderNamespace;
        this.requesterNamespace1 = requesterNamespace1;
        this.requesterNamespace2 = requesterNamespace2;
    }

    public void runMultipleRequesterResponder() {
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("MultipleRequesterResponder-%d")
                .build();

        ExecutorService executor = Executors.newFixedThreadPool(2, threadFactory);

        util.createResponderServer(responderNamespace, (request, responder) -> {
            System.out.print(">>> REQUEST: " + request);

            Future<String> futureRequester1 = createAndRunRequester(executor, requesterNamespace1);
            Future<String> futureRequester2 = createAndRunRequester(executor, requesterNamespace2);

            util.sleep(500);

            String result1 = futureRequester1.get();
            String result2 = futureRequester2.get();

            executor.shutdownNow();

            util.respond(responder, "response from MultipleRequesterResponder:" + (result1 + result2));
        })
        .listen();
    }

    public void shutdown() {
        util.shutdown();
    }

    private Future<String> createAndRunRequester(ExecutorService executor, String namespace) {
        Requester<Payload<Object, Object, Object, Map<String, Object>>> requester = util.createRequester(namespace, NUMBER_OF_RESPONSES, null, 5000);
        Future<String> future = executor.submit(new Callable<String>() {
            String result = null;

            @Override
            public String call() throws Exception {
                util.sendRequest(requester, NUMBER_OF_RESPONSES, response -> {
                    System.out.println(">>> RESPONSE body: " + response.getBody());
                    result = response.getBody().toString();
                    synchronized (this) {
                        notify();
                    }

                });

                synchronized (this) {
                    wait();
                }

                return result;
            }
        });
        return future;
    }
}
