package io.github.tcdl.msb.cli;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.adapters.AdapterFactoryLoader;
import io.github.tcdl.msb.config.MsbConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This tool allows to wiretap the given topics and log the messages from them. See {@link #printUsage()} for parameters supported.
 */
public class CliTool {

    public static void main(String[] args) {
        MsbConfig msbConfig = new MsbConfig(ConfigFactory.load());
        AdapterFactoryLoader adapterFactoryLoader = new AdapterFactoryLoader(msbConfig);

        // Parse command line arguments
        List<String> topics = getTopics(args);
        if (topics == null || topics.isEmpty()) {
            // at least one topic is required
            printUsage();
            return;
        }
        boolean prettyOutput = getPrettyOutput(args);
        List<String> follow = getFollow(args);

        CliMessageSubscriber subscriptionManager = new CliMessageSubscriber(adapterFactoryLoader.getAdapterFactory());
        subscribe(subscriptionManager, topics, follow, prettyOutput);
    }

    private static void printUsage() {
        System.out.println("Usage: CliTool <--topic|-t topic1,topic2> [--pretty true|false] [--follow response]\n"
                + "--topic (required) specifies topic(s) to listen to\n"
                + "--pretty (defaults to 'true') display formatted or not formatted messages\n"
                + "--follow (defaults to 'response') allows to inspect incoming messages and subscribe to response topics automatically");
    }

    static void subscribe(CliMessageSubscriber subscriptionManager, List<String> topics, List<String> follow, boolean prettyOutput) {
        for (String topicName : topics) {
            subscriptionManager.subscribe(topicName, new CliMessageHandler(subscriptionManager, follow, prettyOutput));
        }
    }

    static List<String> getTopics(String[] args) {
        return getOptionAsList(args, "--topic", "-t");
    }

    static List<String> getFollow(String[] args) {
        List<String> follow = getOptionAsList(args, "--follow", "-f");
        if (follow == null || follow.isEmpty()) {
            follow = Collections.singletonList("response");
        }
        return follow;
    }

    static boolean getPrettyOutput(String[] args) {
        return getOptionAsBoolean(args, "--pretty", "-p");
    }

    static boolean getOptionAsBoolean(String[] args, String optionName, String alias) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (arg.equals(optionName) || arg.equals(alias)) {
                if (i + 1 < args.length) {
                    String optionValue = args[i + 1];

                    return optionValue.equals("true");
                }
            }
        }

        return true;
    }

    static List<String> getOptionAsList(String[] args, String optionName, String alias) {
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
