package io.github.tcdl.examples;

import io.github.tcdl.Requester;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by anstr on 6/9/2015.
 */
public class MultipleRequesterResponder extends BaseExample {
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

         createResponderServer(responderNamespace, (request, responder) -> {
                    System.out.print(">>> REQUEST: " + request);

                    Future<String> futureRequester1 = createAndRunRequester(executor, requesterNamespace1, "requester1");
                    Future<String> futureRequester2 = createAndRunRequester(executor, requesterNamespace2, "requester2");

                    sleep(500);

                    String result1 = futureRequester1.get();
                    String result2 = futureRequester2.get();

                    executor.shutdownNow();

                    respond(responder, "response from MultipleRequesterResponder:" + (result1 + result2));
                })
                .listen();
    }

    private Future<String> createAndRunRequester(ExecutorService executor, String namespace, String bodyText) {
        Requester requester = createRequester(namespace, 1, null, 5000);
        Future<String> future = executor.submit(new Callable<String>() {
            String result = null;

            @Override
            public String call() throws Exception {
                sendRequest(requester, 1, response -> {
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
