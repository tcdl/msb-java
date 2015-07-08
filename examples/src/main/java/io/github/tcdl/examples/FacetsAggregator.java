package io.github.tcdl.examples;

import io.github.tcdl.api.MessageTemplate;
import io.github.tcdl.api.MsbContext;
import io.github.tcdl.api.MsbContextBuilder;
import io.github.tcdl.api.message.payload.Payload;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Microservice which is listening for incoming messages, creates requests to another microservices(
 * data-extractor, airport-extractor, resort-extractor), concatenates responses and returns result response
 */
public class FacetsAggregator {

    public static void main(String[] args) {
        MsbContext msbContext = new MsbContextBuilder()
                .withDefaultChannelMonitorAgent(true)
                .withShutdownHook(true)
                .build();

        MessageTemplate options = new MessageTemplate();
        final String namespace = "search:aggregator:facets:v1";

        msbContext.getObjectFactory().createResponderServer(namespace, options, (request, responder) -> {

            RequestQuery query = request.getQueryAs(RequestQuery.class);
            String q = query.getQ();

            if (q == null) {
                Payload responsePayload = new Payload.PayloadBuilder()
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

                Payload responsePayloadAny = new Payload.PayloadBuilder()
                        .withStatusCode(200)
                        .withBody(responseBodyAny).build();

                responder.send(responsePayloadAny);
            } else {
//                responder.send(responsePayload);
            }
        }).listen();
    }

    private static class RequestQuery {
        private String q;

        public String getQ() {
            return q;
        }
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
