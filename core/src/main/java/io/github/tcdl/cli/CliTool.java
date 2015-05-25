package io.github.tcdl.cli;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.config.MsbConfigurations;

import java.util.*;

public class CliTool implements CliMessageHandlerSubscriber {
    private MsbConfigurations configuration;
    private final Set<String> registeredTopics = new HashSet<>();

    public CliTool(MsbConfigurations configuration, List<String> topics, boolean pretty, List<String> follow) {
        this.configuration = configuration;

        // Subscribe to the configured topics
        for (String topicName : topics) {
            subscribe(topicName, new CliMessageHandler(this, pretty, follow));
        }
    }

    @Override
    public void subscribe(String topicName, CliMessageHandler handler) {
        synchronized (registeredTopics) {
            if (!registeredTopics.contains(topicName)) {
                Adapter adapter = getAdapterFactory().createAdapter(configuration.getBrokerType(), topicName, configuration);
                adapter.subscribe(handler);
                registeredTopics.add(topicName);
            }
        }
    }

    public static void main(String[] args) {
        // Parse command-line arguments
        List<String> topics = getOptionAsList(args, "--topic", "-t");
        if (topics == null || topics.isEmpty()) {
            printUsage();
            return;
        }

        List<String> follow = getOptionAsList(args, "--follow", "-f");
        if (follow == null || follow.isEmpty()) {
            follow = Collections.singletonList("response");
        }

        boolean pretty = getOptionAsBoolean(args, "--pretty", "-p");

        new CliTool(MsbConfigurations.msbConfiguration(), topics, pretty, follow);
    }

    private static void printUsage() {
        System.out.println("Usage: CliTool <--topic|-t topic1,topic2> [--pretty true|false] [--follow response]\n"
                + "--topic (required) specifies topic(s) to listen to\n"
                + "--pretty (defaults to 'true') display formatted or not formatted messages\n"
                + "--follow (defaults to 'response') allows to inspect incoming messages and subscribe to response topics automatically");
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
    
    private AdapterFactory getAdapterFactory() {
        return new AdapterFactory();
    }

}
