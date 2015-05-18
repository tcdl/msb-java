package io.github.tcdl.cli;

import io.github.tcdl.ChannelManager;
import io.github.tcdl.Consumer;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.SingleArgumentAdapter;
import io.github.tcdl.messages.Message;

public class CliTool {
    // TODO get rid of this hardcode
    private static final String TOPIC_NAME = "search:parsers:facets:v1";

    public static void main(String[] args) {
        ChannelManager channelManager = ChannelManager.getInstance();

        Consumer consumer = channelManager.findOrCreateConsumer(TOPIC_NAME, null);

        channelManager.on(new Event("message"), new SingleArgumentAdapter<Message>() {
            @Override
            public void onEvent(Message msg) {
                // TODO subscribe to all reply topics as well
                System.out.println(msg.getPayload());
            }
        });
    }
}
