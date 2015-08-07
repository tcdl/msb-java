package io.github.tcdl.msb.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.adapters.mock.MockAdapter;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.api.message.Topics;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.monitor.agent.AgentTopicStats;
import io.github.tcdl.msb.monitor.agent.ChannelMonitorAgent;
import io.github.tcdl.msb.monitor.agent.DefaultChannelMonitorAgent;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;
import org.junit.Before;
import org.junit.Test;

public class ChannelMonitorIT {
    MsbContextImpl msbContext;

    @Before
    public void setUp() {
        msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test
    public void testAnnouncement() throws InterruptedException {
        String TOPIC_NAME = "topic1";
        ChannelMonitorAgent channelMonitorAgent = new DefaultChannelMonitorAgent(msbContext);

        CountDownLatch announcementReceived = new CountDownLatch(1);
        Callback<AggregatorStats> handler = stats -> {
            assertTrue(stats.getTopicInfoMap().containsKey(TOPIC_NAME));
            assertEquals(1, stats.getTopicInfoMap().get(TOPIC_NAME).getProducers().size());
            announcementReceived.countDown();
        };

        ChannelMonitorAggregator channelMonitorAggregator = msbContext.getObjectFactory().createChannelMonitorAggregator(handler);
        channelMonitorAggregator.start(false, ChannelMonitorAggregator.DEFAULT_HEARTBEAT_INTERVAL_MS, ChannelMonitorAggregator.DEFAULT_HEARTBEAT_TIMEOUT_MS);

        channelMonitorAgent.producerTopicCreated(TOPIC_NAME);
        assertTrue("Announcement was not received", announcementReceived.await(RequesterResponderIT.MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testAnnouncementUnexpectedMessage() throws InterruptedException {
        String TOPIC_NAME = "topic1";
        ChannelMonitorAgent channelMonitorAgent = new DefaultChannelMonitorAgent(msbContext);

        CountDownLatch announcementReceived = new CountDownLatch(1);
        Callback<AggregatorStats> handler = stats -> {
            assertTrue(stats.getTopicInfoMap().containsKey(TOPIC_NAME));
            assertEquals(1, stats.getTopicInfoMap().get(TOPIC_NAME).getProducers().size());
            announcementReceived.countDown();
        };

        //simulate unexpectedMessage was consumed from in TOPIC_ANNOUNCE namespace
        MockAdapter.pushRequestMessage(Utils.TOPIC_ANNOUNCE,
                Utils.toJson(TestUtils.createMsbRequestMessageWithSimplePayload(Utils.TOPIC_ANNOUNCE), msbContext.getPayloadMapper()));

        ChannelMonitorAggregator channelMonitorAggregator = msbContext.getObjectFactory().createChannelMonitorAggregator(handler);
        channelMonitorAggregator.start(false, ChannelMonitorAggregator.DEFAULT_HEARTBEAT_INTERVAL_MS, ChannelMonitorAggregator.DEFAULT_HEARTBEAT_TIMEOUT_MS);
        channelMonitorAgent.producerTopicCreated(TOPIC_NAME);

        assertTrue("Announcement was not received", announcementReceived.await(RequesterResponderIT.MESSAGE_TRANSMISSION_TIME * 2, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testHeartbeatMessage() throws InterruptedException {
        String TOPIC_NAME = "topic2";

        Map<String, AgentTopicStats> topicInfoMap = new HashMap<>();
        Instant lastProduced = Instant.now().minusMillis(20000);
        Instant lastConsumed = Instant.now().minusMillis(30000);
        topicInfoMap.put(TOPIC_NAME, new AgentTopicStats(true, false, lastProduced, lastConsumed));

        Payload payload = new Payload.Builder<Object, Object, Object, Map<String, AgentTopicStats>>()
                .withBody(topicInfoMap)
                .build();

        CountDownLatch heartBeatResponseReceived = new CountDownLatch(1);
        Callback<AggregatorStats> handler = stats -> {
            assertTrue(stats.getTopicInfoMap().containsKey(TOPIC_NAME));
            assertEquals(1, stats.getTopicInfoMap().get(TOPIC_NAME).getProducers().size());
            heartBeatResponseReceived.countDown();
        };

        ChannelMonitorAggregator channelMonitorAggregator = msbContext.getObjectFactory().createChannelMonitorAggregator(handler);
        channelMonitorAggregator.start(true, 10000, 1000);

        //need to await for original request for heartbeat to be send to simulate response with same correlationId
        Message requestMessage = awaitHeartBeatRequestSent(RequesterResponderIT.MESSAGE_TRANSMISSION_TIME);

        Message responseMessage = createMsbRequestMessage(requestMessage.getTopics().getResponse(), requestMessage.getCorrelationId(),
                payload);
        MockAdapter.pushRequestMessage(requestMessage.getTopics().getResponse(), Utils.toJson(responseMessage, msbContext.getPayloadMapper()));

        assertTrue("HeartBeat Response was not received",
                heartBeatResponseReceived.await(RequesterResponderIT.MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testHeartbeatUnexpectedMessage() throws InterruptedException {

        CountDownLatch heartBeatResponseReceived = new CountDownLatch(1);
        Callback<AggregatorStats> handler = stats -> {
            assertEquals(0, stats.getTopicInfoMap().size());
            heartBeatResponseReceived.countDown();
        };

        ChannelMonitorAggregator channelMonitorAggregator = msbContext.getObjectFactory().createChannelMonitorAggregator(handler);
        channelMonitorAggregator.start(true, ChannelMonitorAggregator.DEFAULT_HEARTBEAT_INTERVAL_MS, 1000);

        //need to await for original request for heartbeat to be send to simulate response with same correlationId
        Message requestMessage = awaitHeartBeatRequestSent(RequesterResponderIT.MESSAGE_TRANSMISSION_TIME);

        Message responseMessage = createMsbRequestMessage(requestMessage.getTopics().getResponse(), requestMessage.getCorrelationId(),
                " unexpected statistics format received");
        MockAdapter.pushRequestMessage(requestMessage.getTopics().getResponse(), Utils.toJson(responseMessage, msbContext.getPayloadMapper()));

        assertTrue("HeartBeat Response was not received",
                heartBeatResponseReceived.await(RequesterResponderIT.MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
    }

    private Message awaitHeartBeatRequestSent(long timeout) {
        long startTime = System.currentTimeMillis();
        String originalRequest = null;
        while (originalRequest == null && (System.currentTimeMillis() - startTime) < timeout) {
            originalRequest = MockAdapter.pollJsonMessageForTopic(Utils.TOPIC_HEARTBEAT);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        Message requestMessage = Utils.fromJson(originalRequest, Message.class, msbContext.getPayloadMapper());
        return requestMessage;
    }

    private Message createMsbRequestMessage(String topicTo, String correlationId, String payloadString) {
        try {
            ObjectMapper payloadMapper = msbContext.getPayloadMapper();
            MsbConfig msbConf = msbContext.getMsbConfig();
            Clock clock = Clock.systemDefaultZone();
            JsonNode payload = payloadMapper.readValue(String.format("{\"body\": \"%s\" }", payloadString), JsonNode.class);

            Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
            MetaMessage.Builder metaBuilder = TestUtils.createSimpleMetaBuilder(msbConf, clock);
            return new Message.Builder()
                    .withCorrelationId(correlationId)
                    .withId(Utils.generateId())
                    .withTopics(topic)
                    .withMetaBuilder(metaBuilder)
                    .withPayload(payload)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare request message", e);
        }
    }

    private Message createMsbRequestMessage(String topicTo, String correlationId, Payload payload) {

        ObjectMapper payloadMapper = msbContext.getPayloadMapper();
        MsbConfig msbConf = msbContext.getMsbConfig();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
        MetaMessage.Builder metaBuilder = TestUtils.createSimpleMetaBuilder(msbConf, clock);
        return new Message.Builder()
                .withCorrelationId(correlationId)
                .withId(Utils.generateId())
                .withTopics(topic)
                .withMetaBuilder(metaBuilder)
                .withPayload(Utils.convert(payload, JsonNode.class, payloadMapper))
                .build();
    }
}
