package io.github.tcdl.monitor;

import io.github.tcdl.*;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static io.github.tcdl.support.Utils.TOPIC_ANNOUNCE;
import static io.github.tcdl.support.Utils.TOPIC_HEARTBEAT;
import static io.github.tcdl.support.Utils.isServiceTopic;

/**
 * This implementation maintains statistics over all topics. It broadcasts that statistics over the bus for special monitoring microservices. The overall
 * process consists of the following steps:
 *
 * 1. The agent sends an announcement message each time when a new consumer or producer is created for some topic
 * 2. The agent listens on special heartbeat topic for periodic heartbeat messages
 * 3. The agent sends the current statistics in response to the heartbeat.
 */
public class DefaultChannelMonitorAgent implements ChannelMonitorAgent {
    private MsbContext msbContext;
    private ChannelManager channelManager;
    private MessageFactory messageFactory;
    private Clock clock;

    /**
     * This map contains statistics info per topic.
     */
    Map<String, TopicStats> topicInfoMap = new HashMap<>();

    public DefaultChannelMonitorAgent(MsbContext msbContext) {
        this.msbContext = msbContext;

        this.channelManager = msbContext.getChannelManager();
        this.messageFactory = msbContext.getMessageFactory();
        this.clock = msbContext.getClock();
    }

    /**
     * Convenience factory method that creates the agent instance and starts it.
     */
    public static void start(MsbContext msbContext) {
        new DefaultChannelMonitorAgent(msbContext).start();
    }

    /**
     * Start listening on the heartbeat topic and injects itself in the channel manager instance.
     * 
     * @return this channel manages instance. Might be useful for chaining calls.
     */
    public DefaultChannelMonitorAgent start() {
        channelManager.subscribe(TOPIC_HEARTBEAT, // Launch listener for heartbeat topic
        (message, error) -> {
            if (error != null) {
                // TODO
                return;
            }

            Responder responder = new Responder(null, message, msbContext);
            Payload payload = new Payload.PayloadBuilder()
                    .setBody(topicInfoMap)
                    .build();

            responder.send(payload);
        });
        channelManager.setChannelMonitorAgent(this); // Inject itself in channel manager

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public void producerTopicCreated(String topicName) {
        if (isServiceTopic(topicName)) {
            return;
        }

        TopicStats updatedStats = getOrCreateTopicStats(topicName).setProducers(true);
        topicInfoMap.put(topicName, updatedStats);

        doAnnounce();
    }

    /** {@inheritDoc} */
    @Override
    public void consumerTopicCreated(String topicName) {
        if (isServiceTopic(topicName)) {
            return;
        }

        TopicStats updatedStats = getOrCreateTopicStats(topicName).setConsumers(true);
        topicInfoMap.put(topicName, updatedStats);

        doAnnounce();
    }

    /** {@inheritDoc} */
    @Override
    public void consumerTopicRemoved(String topicName) {
        if (isServiceTopic(topicName)) {
            return;
        }

        TopicStats updatedStats = getOrCreateTopicStats(topicName).setConsumers(false);
        topicInfoMap.put(topicName, updatedStats);
    }

    /** {@inheritDoc} */
    @Override
    public void producerMessageSent(String topicName) {
        if (isServiceTopic(topicName)) {
            return;
        }

        Instant now = clock.instant();
        TopicStats updatedStats = getOrCreateTopicStats(topicName).setLastProducedAt(now);
        topicInfoMap.put(topicName, updatedStats);
    }

    /** {@inheritDoc} */
    @Override
    public void consumerMessageReceived(String topicName) {
        if (isServiceTopic(topicName)) {
            return;
        }

        Instant now = clock.instant();
        TopicStats updatedStats = getOrCreateTopicStats(topicName).setLastConsumedAt(now);
        topicInfoMap.put(topicName, updatedStats);
    }

    private TopicStats getOrCreateTopicStats(String topicName) {
        if (!topicInfoMap.containsKey(topicName)) {
            topicInfoMap.put(topicName, new TopicStats());
        }

        return topicInfoMap.get(topicName);
    }

    /**
     * Makes broadcast of the current statistics.
     */
    private void doAnnounce() {
        Payload payload = new Payload.PayloadBuilder()
                .setBody(topicInfoMap)
                .build();

        Producer producer = channelManager.findOrCreateProducer(TOPIC_ANNOUNCE);

        MessageBuilder messageBuilder = messageFactory.createBroadcastMessageBuilder(new MsbMessageOptions(), TOPIC_ANNOUNCE, payload);
        Message announcementMessage = messageBuilder.build();

        producer.publish(announcementMessage);
    }
}
