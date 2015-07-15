package io.github.tcdl.msb.monitor.agent;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.impl.ResponderImpl;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.message.MessageFactory;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.support.Utils;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * This implementation maintains statistics over all topics. It broadcasts that statistics over the bus for special monitoring microservices. The overall
 * process consists of the following steps:
 *
 * 1. The agent sends an announcement message each time when a new consumer or producer is created for some topic
 * 2. The agent listens on special heartbeat topic for periodic heartbeat messages
 * 3. The agent sends the current statistics in response to the heartbeat.
 */
public class DefaultChannelMonitorAgent implements ChannelMonitorAgent {
    private MsbContextImpl msbContext;
    private ChannelManager channelManager;
    private MessageFactory messageFactory;
    private Clock clock;

    /**
     * This map contains statistics info per topic.
     */
    Map<String, AgentTopicStats> topicInfoMap = new HashMap<>();

    public DefaultChannelMonitorAgent(MsbContextImpl msbContext) {
        this.msbContext = msbContext;

        this.channelManager = msbContext.getChannelManager();
        this.messageFactory = msbContext.getMessageFactory();
        this.clock = msbContext.getClock();
    }

    /**
     * Convenience factory method that creates the agent instance and starts it.
     */
    public static void start(MsbContextImpl msbContext) {
        new DefaultChannelMonitorAgent(msbContext).start();
    }

    /**
     * Start listening on the heartbeat topic and injects itself in the channel manager instance.
     * 
     * @return this channel manages instance. Might be useful for chaining calls.
     */
    public DefaultChannelMonitorAgent start() {
        channelManager.subscribe(Utils.TOPIC_HEARTBEAT, // Launch listener for heartbeat topic
                message -> {
                        Responder responder = new ResponderImpl(null, message, msbContext);
                        Payload payload = new Payload.Builder()
                                .withBody(topicInfoMap)
                                .build();
                        responder.send(payload);
                });

        channelManager.setChannelMonitorAgent(this); // Inject itself in channel manager

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public void producerTopicCreated(String topicName) {
        if (Utils.isServiceTopic(topicName)) {
            return;
        }

        AgentTopicStats updatedStats = getOrCreateTopicStats(topicName).withProducers(true);
        topicInfoMap.put(topicName, updatedStats);

        doAnnounce();
    }

    /** {@inheritDoc} */
    @Override
    public void consumerTopicCreated(String topicName) {
        if (Utils.isServiceTopic(topicName)) {
            return;
        }

        AgentTopicStats updatedStats = getOrCreateTopicStats(topicName).withConsumers(true);
        topicInfoMap.put(topicName, updatedStats);

        doAnnounce();
    }

    /** {@inheritDoc} */
    @Override
    public void consumerTopicRemoved(String topicName) {
        if (Utils.isServiceTopic(topicName)) {
            return;
        }

        AgentTopicStats updatedStats = getOrCreateTopicStats(topicName).withConsumers(false);
        topicInfoMap.put(topicName, updatedStats);
    }

    /** {@inheritDoc} */
    @Override
    public void producerMessageSent(String topicName) {
        if (Utils.isServiceTopic(topicName)) {
            return;
        }

        Instant now = clock.instant();
        AgentTopicStats updatedStats = getOrCreateTopicStats(topicName).withLastProducedAt(now);
        topicInfoMap.put(topicName, updatedStats);
    }

    /** {@inheritDoc} */
    @Override
    public void consumerMessageReceived(String topicName) {
        if (Utils.isServiceTopic(topicName)) {
            return;
        }

        Instant now = clock.instant();
        AgentTopicStats updatedStats = getOrCreateTopicStats(topicName).withLastConsumedAt(now);
        topicInfoMap.put(topicName, updatedStats);
    }

    private AgentTopicStats getOrCreateTopicStats(String topicName) {
        if (!topicInfoMap.containsKey(topicName)) {
            topicInfoMap.put(topicName, new AgentTopicStats());
        }

        return topicInfoMap.get(topicName);
    }

    /**
     * Makes broadcast of the current statistics.
     */
    private void doAnnounce() {
        Payload payload = new Payload.Builder()
                .withBody(topicInfoMap)
                .build();

        Producer producer = channelManager.findOrCreateProducer(Utils.TOPIC_ANNOUNCE);

        Message.Builder messageBuilder = messageFactory.createBroadcastMessageBuilder(new MessageTemplate(), Utils.TOPIC_ANNOUNCE, payload);
        Message announcementMessage = messageBuilder.build();

        producer.publish(announcementMessage);
    }
}
