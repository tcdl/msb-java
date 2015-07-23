package io.github.tcdl.msb.monitor.aggregator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.api.message.payload.PayloadWrapper;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.AggregatorTopicStats;
import io.github.tcdl.msb.config.ServiceDetails;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.monitor.agent.AgentTopicStats;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator.DEFAULT_HEARTBEAT_TIMEOUT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DefaultChannelMonitorAggregatorTest {

    private ChannelManager mockChannelManager = mock(ChannelManager.class);
    private ObjectFactory mockObjectFactory = mock(ObjectFactory.class);

    private MsbContextImpl testMsbContext = TestUtils.createMsbContextBuilder()
            .withObjectFactory(mockObjectFactory)
            .withChannelManager(mockChannelManager)
            .build();

    private ScheduledExecutorService mockScheduledExecutorService = mock(ScheduledExecutorService.class);
    @SuppressWarnings("unchecked")
    private Callback<AggregatorStats> mockHandler = mock(Callback.class);
    private DefaultChannelMonitorAggregator channelMonitor = new DefaultChannelMonitorAggregator(testMsbContext, mockScheduledExecutorService, mockHandler);

    @Test
    public void testOnAnnounce() {
        String TOPIC1 = "topic1";
        String TOPIC2 = "topic2";
        String INSTANCE_ID = "instanceId";
        Message announcementMessage = createAnnouncementMessageWith2Topics(INSTANCE_ID, TOPIC1, TOPIC2);

        channelMonitor.onAnnounce(announcementMessage);

        assertEquals(1, channelMonitor.masterAggregatorStats.getServiceDetailsById().size());
        assertTrue(channelMonitor.masterAggregatorStats.getServiceDetailsById().containsKey(INSTANCE_ID));
        assertEquals(2, channelMonitor.masterAggregatorStats.getTopicInfoMap().size());

        verify(mockHandler).call(channelMonitor.masterAggregatorStats);
    }

    @Test
    public void testOnHeartbeatResponses() {
        String INSTANCE_ID_OBSOLETE = "instance_id_obsolete";
        String TOPIC_OBSOLETE = "topic_obsolete";

        // Prepopulate stats to verify that obsolete data are cleared upon heartbeats
        channelMonitor.masterAggregatorStats.getTopicInfoMap().put(TOPIC_OBSOLETE, new AggregatorTopicStats());
        channelMonitor.masterAggregatorStats.getServiceDetailsById().put(INSTANCE_ID_OBSOLETE, new ServiceDetails.Builder().build());

        String INSTANCE_ID_1 = "instanceId1";
        String INSTANCE_ID_2 = "instanceId2";
        String TOPIC1 = "topic1";
        String TOPIC2 = "topic2";

        Message hbMessage1 = createAnnouncementMessageWith2Topics(INSTANCE_ID_1, TOPIC1, TOPIC2);
        Message hbMessage2 = createAnnouncementMessageWith2Topics(INSTANCE_ID_2, TOPIC1, TOPIC2);

        channelMonitor.onHeartbeatResponses(Arrays.asList(hbMessage1, hbMessage2));

        assertEquals(2, channelMonitor.masterAggregatorStats.getServiceDetailsById().size());
        assertTrue(channelMonitor.masterAggregatorStats.getServiceDetailsById().containsKey(INSTANCE_ID_1));
        assertTrue(channelMonitor.masterAggregatorStats.getServiceDetailsById().containsKey(INSTANCE_ID_2));

        assertEquals(2, channelMonitor.masterAggregatorStats.getTopicInfoMap().size());
        assertTrue(channelMonitor.masterAggregatorStats.getTopicInfoMap().containsKey(TOPIC1));
        assertTrue(channelMonitor.masterAggregatorStats.getTopicInfoMap().containsKey(TOPIC2));

        verify(mockHandler, times(1)).call(channelMonitor.masterAggregatorStats);
    }

    @Test
    public void testAggregateInfo() {
        String INSTANCE_ID = "instanceId";
        String TOPIC1 = "topic1";
        String TOPIC2 = "topic2";

        Message announcementMessage = createAnnouncementMessageWith2Topics(INSTANCE_ID, TOPIC1, TOPIC2);

        AggregatorStats aggregatorStats = new AggregatorStats();
        channelMonitor.aggregateInfo(aggregatorStats, announcementMessage);

        assertEquals(1, aggregatorStats.getServiceDetailsById().size());
        assertTrue(aggregatorStats.getServiceDetailsById().containsKey(INSTANCE_ID));
        assertEquals(2, aggregatorStats.getTopicInfoMap().size());
    }

    @Test
    public void testAggregateTopicStatsInitial() {
        // Given
        String INSTANCE_ID = "instanceId";
        String TOPIC_1 = "topic1";
        String TOPIC_2 = "topic2";
        Instant LAST_PRODUCED_AT = Instant.parse("2007-12-03T10:15:30.00Z");
        Instant LAST_CONSUMED_AT = Instant.parse("2007-12-03T10:15:32.00Z");

        Map<String, AgentTopicStats> agentTopicStats = ImmutableMap.of(
                TOPIC_1,
                new AgentTopicStats()
                        .withProducers(true)
                        .withLastProducedAt(LAST_PRODUCED_AT),

                TOPIC_2,
                new AgentTopicStats()
                        .withConsumers(true)
                        .withLastConsumedAt(LAST_CONSUMED_AT)
        );

        // When
        AggregatorStats aggregatorStats = new AggregatorStats();
        channelMonitor.aggregateTopicStats(aggregatorStats, agentTopicStats, INSTANCE_ID);

        // Then
        Map<String, AggregatorTopicStats> topicInfoMap = aggregatorStats.getTopicInfoMap();
        Map<String, AggregatorTopicStats> expectedTopicInfoMap = ImmutableMap.of(
                TOPIC_1,
                new AggregatorTopicStats()
                        .withProducers(ImmutableSet.of(INSTANCE_ID))
                        .withConsumers(Collections.<String>emptySet())
                        .withLastProducedAt(LAST_PRODUCED_AT)
                        .withLastConsumedAt(null),

                TOPIC_2,
                new AggregatorTopicStats()
                        .withProducers(Collections.<String>emptySet())
                        .withConsumers(ImmutableSet.of(INSTANCE_ID))
                        .withLastProducedAt(null)
                        .withLastConsumedAt(LAST_CONSUMED_AT)
        );

        assertEquals(expectedTopicInfoMap, topicInfoMap);
    }

    @Test
    public void testAggregateTopicStatsServiceProducerInstances() {
        String INSTANCE_ID_1 = "instanceId";
        String INSTANCE_ID_2 = "instanceId2";
        String TOPIC = "topic1";

        AggregatorStats aggregatorStats = new AggregatorStats();
        // Process message from one instance
        assertProducers(aggregatorStats, ImmutableSet.of(INSTANCE_ID_1), INSTANCE_ID_1, TOPIC);
        // Process message from another instance
        assertProducers(aggregatorStats, ImmutableSet.of(INSTANCE_ID_1, INSTANCE_ID_2), INSTANCE_ID_2, TOPIC);
        // Process message from the first instance again
        assertProducers(aggregatorStats, ImmutableSet.of(INSTANCE_ID_1, INSTANCE_ID_2), INSTANCE_ID_1, TOPIC);

        // Verify that consumers left untouched
        assertTrue(aggregatorStats.getTopicInfoMap().get(TOPIC).getConsumers().isEmpty());
    }

    @Test
    public void testAggregateTopicStatsServiceConsumerInstances() {
        String INSTANCE_ID_1 = "instanceId";
        String INSTANCE_ID_2 = "instanceId2";
        String TOPIC = "topic1";

        AggregatorStats aggregatorStats = new AggregatorStats();
        // Process message from one instance
        assertConsumers(aggregatorStats, ImmutableSet.of(INSTANCE_ID_1), INSTANCE_ID_1, TOPIC);
        // Process message from another instance
        assertConsumers(aggregatorStats, ImmutableSet.of(INSTANCE_ID_1, INSTANCE_ID_2), INSTANCE_ID_2, TOPIC);
        // Process message from the first instance again
        assertConsumers(aggregatorStats, ImmutableSet.of(INSTANCE_ID_1, INSTANCE_ID_2), INSTANCE_ID_1, TOPIC);

        // Verify that consumers left untouched
        assertTrue(aggregatorStats.getTopicInfoMap().get(TOPIC).getProducers().isEmpty());
    }

    @Test
    public void testAggregateTopicStatsProducingTime() {
        String INSTANCE_ID = "instanceId";
        String TOPIC = "topic1";
        Instant LAST_PRODUCED_AT_OLDER = Instant.parse("2007-12-03T10:15:30.00Z");
        Instant LAST_PRODUCED_AT_NEWER = Instant.parse("2007-12-03T10:15:31.00Z");
        Instant LAST_PRODUCED_AT_NEWEST = Instant.parse("2007-12-03T10:15:32.00Z");

        AggregatorStats aggregatorStats = new AggregatorStats();
        assertLastProducedAt(aggregatorStats, LAST_PRODUCED_AT_NEWER, LAST_PRODUCED_AT_NEWER, INSTANCE_ID, TOPIC);
        // Process message with newer date
        assertLastProducedAt(aggregatorStats, LAST_PRODUCED_AT_NEWEST, LAST_PRODUCED_AT_NEWEST, INSTANCE_ID, TOPIC);
        // Process message with older date
        assertLastProducedAt(aggregatorStats, LAST_PRODUCED_AT_NEWEST, LAST_PRODUCED_AT_OLDER, INSTANCE_ID, TOPIC);
        // Process message with null date
        assertLastProducedAt(aggregatorStats, LAST_PRODUCED_AT_NEWEST, null, INSTANCE_ID, TOPIC);
    }

    @Test
    public void testAggregateTopicStatsConsumingTime() {
        String INSTANCE_ID = "instanceId";
        String TOPIC = "topic1";
        Instant LAST_CONSUMED_AT_OLDER = Instant.parse("2007-12-03T10:15:30.00Z");
        Instant LAST_CONSUMED_AT_NEWER = Instant.parse("2007-12-03T10:15:31.00Z");
        Instant LAST_CONSUMED_AT_NEWEST = Instant.parse("2007-12-03T10:15:32.00Z");

        AggregatorStats aggregatorStats = new AggregatorStats();
        assertLastConsumedAt(aggregatorStats, LAST_CONSUMED_AT_NEWER, LAST_CONSUMED_AT_NEWER, INSTANCE_ID, TOPIC);
        // Process message with newer date
        assertLastConsumedAt(aggregatorStats, LAST_CONSUMED_AT_NEWEST, LAST_CONSUMED_AT_NEWEST, INSTANCE_ID, TOPIC);
        // Process message with older date
        assertLastConsumedAt(aggregatorStats, LAST_CONSUMED_AT_NEWEST, LAST_CONSUMED_AT_OLDER, INSTANCE_ID, TOPIC);
        // Process message with null date
        assertLastConsumedAt(aggregatorStats, LAST_CONSUMED_AT_NEWEST, null, INSTANCE_ID, TOPIC);
    }

    @Test
    public void testStartWithHeartbeat() {
        channelMonitor.start(true, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_TIMEOUT_MS);

        verify(mockChannelManager).subscribe(eq(Utils.TOPIC_ANNOUNCE), any(MessageHandler.class));
        verify(mockScheduledExecutorService).scheduleAtFixedRate(any(HeartbeatTask.class), eq(0L), eq(DEFAULT_HEARTBEAT_INTERVAL_MS),
                eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testStartWithoutHeartbeat() {
        channelMonitor.start(false, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_TIMEOUT_MS);

        verify(mockChannelManager).subscribe(eq(Utils.TOPIC_ANNOUNCE), any(MessageHandler.class));
        verifyNoMoreInteractions(mockScheduledExecutorService);
    }

    @Test
    public void testStop() {
        channelMonitor.start(true, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_TIMEOUT_MS);
        channelMonitor.stop();

        verify(mockChannelManager).unsubscribe(Utils.TOPIC_ANNOUNCE);
        verify(mockScheduledExecutorService).shutdown();
    }

    private void assertProducers(AggregatorStats aggregatorStats, Set<String> expectedProducers, String producerId, String topic) {
        Map<String, AgentTopicStats> agentTopicStats = ImmutableMap.of(topic, new AgentTopicStats().withProducers(true));
        channelMonitor.aggregateTopicStats(aggregatorStats, agentTopicStats, producerId);
        Map<String, AggregatorTopicStats> topicInfoMap = aggregatorStats.getTopicInfoMap();
        assertEquals(expectedProducers, topicInfoMap.get(topic).getProducers());
    }

    private void assertConsumers(AggregatorStats aggregatorStats, Set<String> expectedConsumers, String consumerId, String topic) {
        Map<String, AgentTopicStats> agentTopicStats = ImmutableMap.of(topic, new AgentTopicStats().withConsumers(true));
        channelMonitor.aggregateTopicStats(aggregatorStats, agentTopicStats, consumerId);
        Map<String, AggregatorTopicStats> topicInfoMap = aggregatorStats.getTopicInfoMap();
        assertEquals(expectedConsumers, topicInfoMap.get(topic).getConsumers());
    }

    private void assertLastProducedAt(AggregatorStats aggregatorStats, Instant expectedlastProducedAt, Instant lastProducedAt, String instanceId, String topic) {
        Map<String, AgentTopicStats> agentTopicStats = ImmutableMap.of(topic, new AgentTopicStats()
                .withProducers(true)
                .withLastProducedAt(lastProducedAt));
        channelMonitor.aggregateTopicStats(aggregatorStats, agentTopicStats, instanceId);
        assertEquals(expectedlastProducedAt, aggregatorStats.getTopicInfoMap().get(topic).getLastProducedAt());
    }

    private void assertLastConsumedAt(AggregatorStats aggregatorStats, Instant expectedLastConsumedAt, Instant lastConsumedAt, String instanceId, String topic) {
        Map<String, AgentTopicStats> agentTopicStats = ImmutableMap.of(topic, new AgentTopicStats()
                .withConsumers(true)
                .withLastConsumedAt(lastConsumedAt));
        channelMonitor.aggregateTopicStats(aggregatorStats, agentTopicStats, instanceId);
        assertEquals(expectedLastConsumedAt, aggregatorStats.getTopicInfoMap().get(topic).getLastConsumedAt());
    }

    private Message createAnnouncementMessageWith2Topics(String instanceId, String topic1, String topic2) {
        Map<String, AgentTopicStats> topicInfoMap = new HashMap<>();
        topicInfoMap.put(topic1, new AgentTopicStats().withProducers(true).withLastProducedAt(Instant.now()));
        topicInfoMap.put(topic2, new AgentTopicStats().withConsumers(true).withLastConsumedAt(Instant.now()));

        Payload announcementPayload = new Payload.Builder()
                .withBody(topicInfoMap)
                .build();

        Payload wrappedPayload = PayloadWrapper.wrap(announcementPayload, TestUtils.createMessageMapper());

        return TestUtils.createMsbRequestMessage("to", instanceId, wrappedPayload);
    }

}