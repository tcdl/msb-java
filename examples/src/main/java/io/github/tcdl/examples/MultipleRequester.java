package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.Requester;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Created by rdrozdov-tc on 6/8/15.
 */
public class MultipleRequester {

    public static void main(String... args) throws Exception {
        MsbContext msbContext = new MsbContext.MsbContextBuilder().build();

        int numberOfRequesters = 5;
        CountDownLatch countDownLatch = new CountDownLatch(numberOfRequesters);

        ConcurrentLinkedDeque<String> requests = new ConcurrentLinkedDeque<>();
        for (int i = 0; i < numberOfRequesters; i++) {
            String requestId = "request#" + (i+1);
            requests.add(requestId);
            runRequester("test:aggregator", requestId, "Holidays in Spain", msbContext,  body -> {
                String confirmedRequestId = (String)body.get("requestId");
                System.out.println(">>> RECEIVED response for " + confirmedRequestId);
                requests.remove(requestId);
                countDownLatch.countDown();
            });
        }

        countDownLatch.await(30000, TimeUnit.MILLISECONDS);
        System.out.println(">>> UN-RESPONDED requests: " + requests.size());
    }

    public static void runRequester(String namespace, String requestId, String queryString, MsbContext msbContext, Consumer<Map> callback) {

        MsbMessageOptions options = new MsbMessageOptions();
        options.setNamespace(namespace);
        options.setWaitForResponses(1);
        options.setAckTimeout(10000);
        options.setResponseTimeout(10000);

        SearchRequest request = new SearchRequest(requestId, queryString);
        Payload requestPayload = new Payload.PayloadBuilder().setBody(request).build();

        CompletableFuture.supplyAsync(() -> {
            Requester.create(options, msbContext)
                .onAcknowledge(acknowledge ->
                    System.out.println(">>> ACK timeout: " + acknowledge.getTimeoutMs())
                )
                .onResponse(payload -> {
                    System.out.println(">>> RESPONSE body: " + payload.getBody());
                    callback.accept(payload.getBody());
                })
                .publish(requestPayload);
            return null;
        });
    }

    public static class SearchRequest {

        private String requestId;
        private String query;

        public SearchRequest() {
        }

        public SearchRequest(String requestId, String query) {
            this.requestId = requestId;
            this.query = query;
        }

        public String getRequestId() {
            return requestId;
        }

        public void setRequestId(String requestId) {
            this.requestId = requestId;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }
    }
}
