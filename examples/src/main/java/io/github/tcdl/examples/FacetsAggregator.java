package io.github.tcdl.examples;

import io.github.tcdl.api.*;
import io.github.tcdl.api.message.Message;
import io.github.tcdl.api.message.payload.Payload;
import io.nodyn.NoOpExitHandler;
import io.nodyn.Nodyn;
import io.nodyn.runtime.NodynConfig;
import io.nodyn.runtime.RuntimeFactory;

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

    private static final String SCRIPT = "" +
            "var executor = require('autoCorrect.js');" +
            "executor.load();";

    public static void main(String[] args) throws ScriptException, FileNotFoundException, NoSuchMethodException {

        MsbContext msbContext = new MsbContextBuilder()
                .withDefaultChannelMonitorAgent(true)
                .withShutdownHook(true)
                .build();

        MessageTemplate options = new MessageTemplate();
        final String namespace = "search:aggregator:facets:v1";

        autoCorrect("autoCorrect");

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
                RequestOptions requestOptions = new RequestOptions.Builder()
                        .withWaitForResponses(1)
                        .withAckTimeout(200)
                        .withResponseTimeout(600)
                        .build();

                Requester requester = msbContext.getObjectFactory().createRequester("search:parsers:facets:v1",
                        requestOptions, responder.getOriginalMessage());

                final String[] result = {""};

                requester.onResponse(response -> {
                    System.out.println(">>> RESPONSE: " + response);
                    result[0] +=response;
                });

                requester.onEnd(listOfMessages -> {
                    for (Message message : listOfMessages)
                        System.out.println(">>> MESSAGE: " + message.getPayload().getBody());

                    Payload responsePayload = new Payload.PayloadBuilder()
                            .withStatusCode(200)
                            .withBody(result[0]).build();

                    responder.send(responsePayload);
                });

                requester.publish(request);
            }
        }).listen();
    }

    private static String autoCorrect(String s) {
        System.setProperty( "nodyn.binary", "node" );
        // Use DynJS runtime
        RuntimeFactory factory = RuntimeFactory.init(
                FacetsAggregator.class.getClassLoader(),
                RuntimeFactory.RuntimeType.DYNJS);

        // Set config to run main.js
        NodynConfig config = new NodynConfig( new String[] { "-e", SCRIPT } );

        // Create a new Nodyn and run it
        Nodyn nodyn = factory.newRuntime(config);
        nodyn.setExitHandler( new NoOpExitHandler() );
        try {
            int exitCode = nodyn.run();
            if (exitCode != 0) {
                throw new RuntimeException();
            }
        } catch (Throwable t) {
            throw new RuntimeException( t );
        }

        return null;
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
