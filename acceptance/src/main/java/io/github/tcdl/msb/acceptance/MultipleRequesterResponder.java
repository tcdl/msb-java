package io.github.tcdl.msb.acceptance;

import io.github.tcdl.msb.api.Requester;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

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

        util.createResponderServer(responderNamespace, (request, responderContext) -> {
            System.out.print(">>> REQUEST: " + request);

            Future<String> futureRequester1 = createAndRunRequester(executor, requesterNamespace1);
            Future<String> futureRequester2 = createAndRunRequester(executor, requesterNamespace2);

            Thread.sleep(500);

            String result1 = futureRequester1.get();
            String result2 = futureRequester2.get();

            executor.shutdownNow();

            responderContext.getResponder().send("response from MultipleRequesterResponder:" + (result1 + result2));
        }, String.class)
        .listen();
    }

    public void shutdown() {
        util.shutdown();
    }

    private Future<String> createAndRunRequester(ExecutorService executor, String namespace) {
        Requester<String> requester = util.createRequester(namespace, NUMBER_OF_RESPONSES, null, 5000, String.class);
        Future<String> future = executor.submit(new Callable<String>() {
            String result = null;

            @Override
            public String call() throws Exception {
                util.sendRequest(requester, "PING", NUMBER_OF_RESPONSES, response -> {
                    System.out.println(">>> RESPONSE body: " + response);
                    result = response;
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
