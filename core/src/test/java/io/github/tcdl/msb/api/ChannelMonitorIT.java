package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.monitor.agent.ChannelMonitorAgent;
import io.github.tcdl.msb.monitor.agent.DefaultChannelMonitorAgent;
import io.github.tcdl.msb.monitor.aggregator.DefaultChannelMonitorAggregator;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        DefaultChannelMonitorAggregator channelMonitorAggregator = new DefaultChannelMonitorAggregator(msbContext, new ScheduledThreadPoolExecutor(1), handler);
        channelMonitorAggregator.start(false, ChannelMonitorAggregator.DEFAULT_HEARTBEAT_INTERVAL_MS, ChannelMonitorAggregator.DEFAULT_HEARTBEAT_TIMEOUT_MS);

        channelMonitorAgent.producerTopicCreated(TOPIC_NAME);
        assertTrue("Announcement was not received", announcementReceived.await(RequesterResponderIT.MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
    }
}
