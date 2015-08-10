package io.github.tcdl.msb.monitor.aggregator;

import static io.github.tcdl.msb.support.Utils.TOPIC_ANNOUNCE;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.AggregatorTopicStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;
import io.github.tcdl.msb.config.ServiceDetails;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.monitor.agent.AgentTopicStats;
import io.github.tcdl.msb.support.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultChannelMonitorAggregator implements ChannelMonitorAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultChannelMonitorAggregator.class);

    private ChannelManager channelManager;
    private ObjectFactory objectFactory;
    private ObjectMapper messageMapper;
    private ScheduledExecutorService scheduledExecutorService;
    private Callback<AggregatorStats> handler;

    AggregatorStats masterAggregatorStats = new AggregatorStats();

    public DefaultChannelMonitorAggregator(MsbContextImpl msbContext, ScheduledExecutorService scheduledExecutorService,
            Callback<AggregatorStats> aggregatorStatsHandler) {
        this.channelManager = msbContext.getChannelManager();
        this.objectFactory = msbContext.getObjectFactory();
        this.messageMapper = msbContext.getPayloadMapper();
        this.scheduledExecutorService = scheduledExecutorService;
        this.handler = aggregatorStatsHandler;
    }

    @Override
    public void start(boolean activateHeartbeats, long heartbeatIntervalMs, int heartbeatTimeoutMs) {
        channelManager.subscribe(TOPIC_ANNOUNCE, this::onAnnounce);
        LOG.debug(String.format("Subscribed to %s", TOPIC_ANNOUNCE));

        if (activateHeartbeats) {
            Runnable heartbeatTask = new HeartbeatTask(heartbeatTimeoutMs, objectFactory, this::onHeartbeatResponses);
            scheduledExecutorService.scheduleAtFixedRate(heartbeatTask, 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
            LOG.debug("Periodic heartbeats activated");
        }

        LOG.info("DefaultChannelMonitorAggregator started");
    }

    @Override
    public void stop() {
        channelManager.unsubscribe(TOPIC_ANNOUNCE);
        scheduledExecutorService.shutdown();
        LOG.info("DefaultChannelMonitorAggregator stopped");
    }

    void onHeartbeatResponses(List<Message> heartbeatResponses) {
        LOG.debug(String.format("Handling heartbeat responses %s...", heartbeatResponses));
        AggregatorStats aggregatorStats = new AggregatorStats();
        boolean successfullyAggregatedAtLeastOne = false;
        for (Message msg : heartbeatResponses) {
            if (aggregateInfo(aggregatorStats, msg)) {
                successfullyAggregatedAtLeastOne = true;
            }
        }
        if (successfullyAggregatedAtLeastOne) {
            LOG.debug(String.format("Calling registered handler for heartbeat statistics %s...", masterAggregatorStats));
            handler.call(aggregatorStats);
            masterAggregatorStats = aggregatorStats;
            LOG.debug(String.format("Heartbeat responses processed"));
        }
    }

    void onAnnounce(Message announcementMessage) {
        LOG.debug(String.format("Handling announcement message %s...", announcementMessage));

        boolean successfullyAggregated = aggregateInfo(masterAggregatorStats, announcementMessage);

        if (successfullyAggregated) {
            LOG.debug(String.format("Calling registered handler for announcement statistics %s...", masterAggregatorStats));
            handler.call(masterAggregatorStats);
            LOG.debug(String.format("Announcement message processed"));
        }
    }

    boolean aggregateInfo(AggregatorStats aggregatorStats, Message message) {

        JsonNode rawPayload = message.getRawPayload();

        if (!Utils.isPayloadPresent(rawPayload)) {
            LOG.error("Unable to convert message. Message payload is empty.");
            return false;
        }

        try {
            Payload<?, ?, ?, Map<String, AgentTopicStats>> payload = Utils
                    .convert(rawPayload, new TypeReference<Payload<Object, Object, Object, Map<String, AgentTopicStats>>>() {
                    }, messageMapper);
            MetaMessage meta = message.getMeta();
            ServiceDetails serviceDetails = meta.getServiceDetails();
            String instanceId = serviceDetails.getInstanceId();
            aggregatorStats.getServiceDetailsById().put(instanceId, serviceDetails);

            Map<String, AgentTopicStats> agentTopicStatsMap = payload.getBody();
            aggregateTopicStats(aggregatorStats, agentTopicStatsMap, instanceId);
        } catch (JsonConversionException e) {
            LOG.error("Unable to convert message.", e);
            return false;
        }

        return true;
    }

    void aggregateTopicStats(AggregatorStats aggregatorStats, Map<String, AgentTopicStats> agentTopicStatsMap, String instanceId) {
        for (Entry<String, AgentTopicStats> entry : agentTopicStatsMap.entrySet()) {
            String topic = entry.getKey();
            AgentTopicStats agentTopicStats = entry.getValue();

            aggregatorStats.getTopicInfoMap().compute(topic, (topic1, oldValue) -> {
                AggregatorTopicStats newValue = new AggregatorTopicStats(oldValue);

                if (agentTopicStats.isConsumers()) {
                    newValue.getConsumers().add(instanceId);
                    if (newValue.getLastConsumedAt() == null) {
                        newValue.withLastConsumedAt(agentTopicStats.getLastConsumedAt());
                    } else if (agentTopicStats.getLastConsumedAt() != null
                            && agentTopicStats.getLastConsumedAt().isAfter(newValue.getLastConsumedAt())) {
                        newValue.withLastConsumedAt(agentTopicStats.getLastConsumedAt());
                    }
                }

                if (agentTopicStats.isProducers()) {
                    newValue.getProducers().add(instanceId);
                    if (newValue.getLastProducedAt() == null) {
                        newValue.withLastProducedAt(agentTopicStats.getLastProducedAt());
                    } else if (agentTopicStats.getLastProducedAt() != null
                            && agentTopicStats.getLastProducedAt().isAfter(newValue.getLastProducedAt())) {
                        newValue.withLastProducedAt(agentTopicStats.getLastProducedAt());
                    }
                }

                return newValue;
            });
        }
    }
}
