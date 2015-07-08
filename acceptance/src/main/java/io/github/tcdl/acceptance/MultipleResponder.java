package io.github.tcdl.acceptance;

import io.github.tcdl.api.MessageTemplate;
import io.github.tcdl.api.MsbContext;
import io.github.tcdl.api.MsbContextBuilder;
import io.github.tcdl.api.message.payload.Payload;

import java.util.Map;

/**
 * Created by rdrozdov-tc on 6/8/15.
 */
public class MultipleResponder {

    public static void main(String... args) {
        MsbContext msbContext = new MsbContextBuilder()
                .withShutdownHook(true)
                .build();
        runResponder("test:aggregator", msbContext);
    }

    public static void runResponder(String namespace, MsbContext msbContext) {
        MessageTemplate options = new MessageTemplate();
        msbContext.getObjectFactory().createResponderServer(namespace, options, (request, responder) -> {
            Map requestBody = request.getBodyAs(Map.class);
            System.out.println(">>> GOT request: " + requestBody);

            String requestId = (String) requestBody.get("requestId");
            SearchResponse response = new SearchResponse(requestId, "response");
            System.out.println(">>> SENDING response in request to " + requestId);
            responder.send(new Payload.Builder().withBody(response).build());
        })
                .listen();
    }

    public static class SearchResponse {

        private String requestId;
        private String result;

        public SearchResponse() {
        }

        public SearchResponse(String requestId, String result) {
            this.requestId = requestId;
            this.result = result;
        }

        public String getRequestId() {
            return requestId;
        }

        public void setRequestId(String requestId) {
            this.requestId = requestId;
        }

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }

        @Override public String toString() {
            return "response {requestId=" + requestId + ", result=" + result + "}";
        }
    }
}
