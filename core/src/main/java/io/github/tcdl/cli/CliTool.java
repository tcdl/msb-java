package io.github.tcdl.cli;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.json.JSONObject;
import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.config.MsbConfigurations;

import java.io.IOException;
import java.util.*;

public class CliTool {
    private static final AdapterFactory adapterFactory = AdapterFactory.getInstance();
    private static final MsbConfigurations configuration = MsbConfigurations.msbConfiguration();
    private static final MsbConfigurations.BrokerAdapter brokerType = configuration.getBrokerType();
    private static final Set<String> registeredTopics = new HashSet<>();

    public static void main(String[] args) {
        // Parse command-line arguments
        List<String> topics = getOptionAsList(args, "--topic", "-t");
        if (topics == null || topics.isEmpty()) {
            System.out.println("Please specify topic via --topic <topic name>");
            return;
        }

        List<String> follow = getOptionAsList(args, "--follow", "-f");
        if (follow == null || follow.isEmpty()) {
            follow = Collections.singletonList("response");
        }

        boolean pretty = getOptionAsBoolean(args, "--pretty", "-p");

        // Subscribe to the configured topics
        for (String topicName : topics) {
            subscribe(topicName, new CliMessageHandler(pretty, follow));
        }
    }

    private static boolean getOptionAsBoolean(String[] args, String optionName, String alias) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (arg.equals(optionName) || arg.equals(alias)) {
                if (i + 1 < args.length) {
                    String optionValue = args[i + 1];

                    return optionValue.equals("true");
                }
            }
        }

        return false;
    }

    private static List<String> getOptionAsList(String[] args, String optionName, String alias) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (arg.equals(optionName) || arg.equals(alias)) {
                if (i + 1 < args.length) {
                    String optionValue = args[i + 1];

                    return Arrays.asList(optionValue.split(","));
                }
            }
        }

        return null;
    }

    private static void subscribe(String topicName, CliMessageHandler handler) {
        synchronized (registeredTopics) {
            if (!registeredTopics.contains(topicName)) {
                Adapter adapter = adapterFactory.createAdapter(brokerType, topicName, MsbConfigurations.msbConfiguration());
                adapter.subscribe(handler);
                registeredTopics.add(topicName);
            }
        }
    }

    private static class CliMessageHandler implements Adapter.RawMessageHandler {
        private boolean prettyOutput;
        private List<String> follow;

        public CliMessageHandler(boolean prettyOutput, List<String> follow) {
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
                        && follow.contains("response")) {

                    String responseTopicName = jsonMessageObject.getJSONObject("topics").getString("response");

                    subscribe(responseTopicName, new CliMessageHandler(prettyOutput, follow));
                }

                // Reformat json message in pretty way
                Object tmpObject = objectMapper.readValue(jsonMessage, Object.class);
                System.out.println(objectMapper.writeValueAsString(tmpObject));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
