package io.github.tcdl.msb.examples;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.examples.payload.Request;

import javax.script.ScriptException;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Microservice which is listening for incoming messages, creates requests to another microservices(
 * data-extractor, airport-extractor, resort-extractor), concatenates responses and returns result response
 */
public class FacetsAggregator {

    public static void main(String[] args) throws ScriptException, FileNotFoundException, NoSuchMethodException {

        MsbContext msbContext = new MsbContextBuilder()
                .enableChannelMonitorAgent(true)
                .enableShutdownHook(true)
                .build();

        MessageTemplate options = new MessageTemplate();
        final String namespace = "search:aggregator:facets:v1";

        msbContext.getObjectFactory().createResponderServer(namespace, options, (request, responder) -> {

            Request facetsRequest = (Request) request;
            String q = facetsRequest.getQuery().getQ();

            if (q == null) {
                Payload responsePayload = new Payload.Builder()
                        .withStatusCode(400)
                        .build();
                responder.send(responsePayload);
            } else if (q.isEmpty()) {
                ResponseBodyAny responseBodyAny = new ResponseBodyAny();
                Facet facet = new Facet();
                facet.setProbability(1f);

                Map<String, Object> map = new HashMap<>();
                map.put("depAirport", 0);
                map.put("resortCode", "any");
                facet.setParams(map);

                responseBodyAny.setFacets(Arrays.asList(facet));

                Payload responsePayloadAny = new Payload.Builder<Object, Object, Object, ResponseBodyAny>()
                        .withStatusCode(200)
                        .withBody(responseBodyAny)
                        .build();

                responder.send(responsePayloadAny);
            } else {
                RequestOptions requestOptions = new RequestOptions.Builder()
                        .withWaitForResponses(1)
                        .withAckTimeout(200)
                        .withResponseTimeout(600)
                        .build();

                Requester<Payload> requester = msbContext.getObjectFactory().createRequester("search:parsers:facets:v1",
                        requestOptions, Payload.class);

                final String[] result = {""};

                requester.onResponse(response -> {
                    System.out.println(">>> RESPONSE: " + response);
                    result[0] +=response;
                });

                requester.onEnd(listOfMessages -> {
                    for (Message message : listOfMessages)
                        System.out.println(">>> MESSAGE: " + message.getRawPayload());

                    Payload responsePayload = new Payload.Builder<Object, Object, Object, String>()
                            .withStatusCode(200)
                            .withBody(result[0])
                            .build();

                    responder.send(responsePayload);
                });

                requester.publish(request, responder.getOriginalMessage());
            }
        }).listen();
    }

    private static class ResponseBodyAny {
        private List<Facet> facets;

        public List<Facet> getFacets() {
            return facets;
        }

        public void setFacets(List<Facet> facets) {
            this.facets = facets;
        }
    }

    private static class Facet {
        private float probability;
        private int[] offsets = {};
        private Map<String, Object> params;
        private String[] mappings = {};

        public float getProbability() {
            return probability;
        }

        public void setProbability(float probability) {
            this.probability = probability;
        }

        public int[] getOffsets() {
            return offsets;
        }

        public void setOffsets(int[] offsets) {
            this.offsets = offsets;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public void setParams(Map<String, Object> params) {
            this.params = params;
        }

        public String[] getMappings() {
            return mappings;
        }

        public void setMappings(String[] mappings) {
            this.mappings = mappings;
        }
    }
}
