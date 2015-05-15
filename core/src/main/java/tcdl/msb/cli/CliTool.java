package tcdl.msb.cli;

import tcdl.msb.ChannelManager;
import tcdl.msb.Consumer;
import tcdl.msb.events.Event;
import tcdl.msb.events.SingleArgumentAdapter;
import tcdl.msb.messages.Message;

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
