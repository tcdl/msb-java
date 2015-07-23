package io.github.tcdl.msb.monitor.aggregator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.api.message.payload.PayloadWrapper;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.AggregatorTopicStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;
import io.github.tcdl.msb.config.ServiceDetails;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.monitor.agent.AgentTopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.github.tcdl.msb.support.Utils.TOPIC_ANNOUNCE;

public class DefaultChannelMonitorAggregator implements ChannelMonitorAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultChannelMonitorAggregator.class);

    private ChannelManager channelManager;
    private ObjectFactory objectFactory;
    private ObjectMapper messageMapper;
    private ScheduledExecutorService scheduledExecutorService;
    private Callback<AggregatorStats> handler;

    AggregatorStats masterAggregatorStats = new AggregatorStats();

    public DefaultChannelMonitorAggregator(MsbContextImpl msbContext, ScheduledExecutorService scheduledExecutorService, Callback<AggregatorStats> aggregatorStatsHandler) {
        this.channelManager = msbContext.getChannelManager();
        this.objectFactory = msbContext.getObjectFactory();
        this.messageMapper = msbContext.getMessageMapper();
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
        for (Message msg : heartbeatResponses) {
            aggregateInfo(aggregatorStats, msg);
        }
        LOG.debug(String.format("Calling registered handler for heartbeat %s...", heartbeatResponses));
        handler.call(aggregatorStats);
        masterAggregatorStats = aggregatorStats;
        LOG.debug(String.format("Heartbeat message processed %s", heartbeatResponses));
    }

    void onAnnounce(Message announcementMessage) {
        LOG.debug(String.format("Handling announcement message %s...", announcementMessage));
        aggregateInfo(masterAggregatorStats, announcementMessage);
        LOG.debug(String.format("Calling registered handler for announcement %s...", announcementMessage));
        handler.call(masterAggregatorStats);
        LOG.debug(String.format("Announcement message processed %s", announcementMessage));
    }

    void aggregateInfo(AggregatorStats aggregatorStats, Message message) {
        MetaMessage meta = message.getMeta();
        ServiceDetails serviceDetails = meta.getServiceDetails();
        String instanceId = serviceDetails.getInstanceId();
        aggregatorStats.getServiceDetailsById().put(instanceId, serviceDetails);

        Payload payload = PayloadWrapper.wrap(message.getPayload(), messageMapper);
        Map<String, AgentTopicStats> agentTopicStatsMap = payload.getBodyAs(new TypeReference<Map<String, AgentTopicStats>>() {});
        aggregateTopicStats(aggregatorStats, agentTopicStatsMap, instanceId);
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
