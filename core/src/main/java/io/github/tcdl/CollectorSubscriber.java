package io.github.tcdl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.tcdl.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ruslan on 23.06.15.
 */
public class CollectorSubscriber implements  Subscriber {

    private static final Logger LOG = LoggerFactory.getLogger(CollectorSubscriber.class);

    private ChannelManager channelManager;
    Map<String, Collector> collectorsByCorrelationId  = new ConcurrentHashMap<>();
    Map<String, AtomicInteger> collectorsByTopic = new ConcurrentHashMap<>();


    public CollectorSubscriber(ChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    @Override
    public void handleMessage(Message message) {
        String correlationId = message.getCorrelationId();
        Collector collector = collectorsByCorrelationId.get(correlationId);
        if (collector != null) {
            collector.handleMessage(message);
        } else {
            LOG.warn("Message with correlationId {} is not expected to be processed by any Collectors", correlationId) ;
        }

    }

    public synchronized void registerCollector (String topic, Collector collector) {
        collectorsByCorrelationId.putIfAbsent(collector.getRequestMessage().getCorrelationId(), collector);

        AtomicInteger activeCollectorsForTopic = collectorsByTopic.computeIfAbsent(topic, s -> {return new AtomicInteger(0);});
        int activeCollectorsNum = activeCollectorsForTopic.incrementAndGet();
        LOG.debug("Active collectors number for topic [{}] is {} " , topic, activeCollectorsNum);
    }

    /**
     * Unsubscribe from consumer if no more collectors await on consumer topic.
     *
     * @return true if no more collectors await responses on specific topic
     */
    public synchronized boolean unsubscribe(String topic, Collector collector) {
        collectorsByCorrelationId.remove(collector.getRequestMessage().getCorrelationId());
        AtomicInteger activeCollectorsForTopic = collectorsByTopic.get(topic);
        if (activeCollectorsForTopic == null || activeCollectorsForTopic.decrementAndGet() <= 0) {
            channelManager.unsubscribe(topic);
            return true;
        }
        return false;
    }
}
