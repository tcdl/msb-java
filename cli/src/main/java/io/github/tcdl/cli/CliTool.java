package io.github.tcdl.cli;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.adapters.AdapterFactoryLoader;
import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.config.MsbConfigurations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CliTool implements CliMessageHandlerSubscriber {
    private AdapterFactory adapterFactory;
    private final Set<String> registeredTopics = new HashSet<>();

    public CliTool(AdapterFactory adapterFactory, List<String> topics, boolean pretty, List<String> follow) {
        this.adapterFactory = adapterFactory;

        // Subscribe to the configured topics
        for (String topicName : topics) {
            subscribe(topicName, new CliMessageHandler(this, pretty, follow));
        }
    }

    @Override
    public void subscribe(String topicName, CliMessageHandler handler) {
        synchronized (registeredTopics) {
            if (!registeredTopics.contains(topicName)) {
                ConsumerAdapter adapter = adapterFactory.createConsumerAdapter(topicName);
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
        
        new CliTool(new AdapterFactoryLoader(new MsbConfigurations(ConfigFactory.load())).getAdapterFactory(), topics, pretty, follow);
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
    
 }
