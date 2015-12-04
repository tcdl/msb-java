package io.github.tcdl.msb.cli;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.ConsumerAdapter.AcknowledgementHandler;
import io.github.tcdl.msb.api.exception.JsonConversionException;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * This handler dumps messages from the given topics.
 *
 * Additionally it examines the content of messages, looks for any other topics mentioned there (for example response topics) and subscribes to them as well.
 */
class CliMessageHandler implements ConsumerAdapter.RawMessageHandler {
    private CliMessageSubscriber subscriber;
    private boolean prettyOutput;
    private List<String> follow;

    public CliMessageHandler(CliMessageSubscriber subscriber, List<String> follow, boolean prettyOutput) {
        this.subscriber = subscriber;
        this.follow = follow;
        this.prettyOutput = prettyOutput;
    }

    /**
     * @throws JsonConversionException if some problems during parsing JSON
     */
    @Override
    public void onMessage(String jsonMessage, AcknowledgementHandler handler) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        if (prettyOutput) {
            objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        }

        try {
            JsonNode jsonMessageObject = objectMapper.readTree(jsonMessage);

            // Subscribe to additional topics found in the message
            if (jsonMessageObject.has("topics")
                    && jsonMessageObject.get("topics").has("response")
                    && jsonMessageObject.get("topics").get("response").isTextual()
                    && follow.contains("response")) {

                String responseTopicName = jsonMessageObject.get("topics").get("response").asText();

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
            throw new JsonConversionException("Unable to process JSON", e);
        }
    }
}
