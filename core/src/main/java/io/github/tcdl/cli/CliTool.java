package io.github.tcdl.cli;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.config.MsbConfigurations;

import java.util.*;

public class CliTool implements CliMessageHandlerSubscriber {
    private AdapterFactory adapterFactory;
    private MsbConfigurations configuration;
    private final Set<String> registeredTopics = new HashSet<>();

    public CliTool(MsbConfigurations configuration, AdapterFactory adapterFactory, List<String> topics, boolean pretty, List<String> follow) {
        this.adapterFactory = adapterFactory;
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
                Adapter adapter = adapterFactory.createAdapter(configuration.getBrokerType(), topicName, configuration);
                adapter.subscribe(handler);
                registeredTopics.add(topicName);
            }
        }
    }

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

        new CliTool(MsbConfigurations.msbConfiguration(), AdapterFactory.getInstance(), topics, pretty, follow);
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

}
