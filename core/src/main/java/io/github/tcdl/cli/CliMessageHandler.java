package io.github.tcdl.cli;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.tcdl.adapters.Adapter;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;

class CliMessageHandler implements Adapter.RawMessageHandler {
    private CliMessageHandlerSubscriber subscriber;
    private boolean prettyOutput;
    private List<String> follow;

    public CliMessageHandler(CliMessageHandlerSubscriber subscriber, boolean prettyOutput, List<String> follow) {
        this.subscriber = subscriber;
        this.prettyOutput = prettyOutput;
        this.follow = follow;
    }

    @Override
    public void onMessage(String jsonMessage) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        if (prettyOutput) {
            objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        }

        try {
            JSONObject jsonMessageObject = new JSONObject(jsonMessage);

            // Subscribe to additional topics found in the message
            if (jsonMessageObject.has("topics")
                    && jsonMessageObject.getJSONObject("topics").has("response")
                    && jsonMessageObject.getJSONObject("topics").get("response") instanceof String
                    && follow.contains("response")) {

                String responseTopicName = jsonMessageObject.getJSONObject("topics").getString("response");

                try {
                    subscriber.subscribe(responseTopicName, this);
                } catch (Exception e) {
                    // Just ignore the exception
                }
            }

            // Reformat json message in pretty way
            Object tmpObject = objectMapper.readValue(jsonMessage, Object.class);
            System.out.println(objectMapper.writeValueAsString(tmpObject));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}