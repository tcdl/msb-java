package io.github.tcdl.cli;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.tcdl.exception.JsonConversionException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;

import static io.github.tcdl.adapters.ConsumerAdapter.RawMessageHandler;

/**
 * This handler dumps messages from the given topics.
 *
 * Additionally it examines the content of messages, looks for any other topics mentioned there (for example response topics) and subscribes to them as well.
 */
class CliMessageHandler implements RawMessageHandler {
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
            throw new JsonConversionException(e.getMessage());
        }
    }
}
