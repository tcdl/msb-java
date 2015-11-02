package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.PayloadConverter;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;
import io.github.tcdl.msb.monitor.aggregator.DefaultChannelMonitorAggregator;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/**
 * Provides methods for creation {@link Requester} and {@link ResponderServer}.
 */
public class ObjectFactoryImpl implements ObjectFactory {
    private static Logger LOG = LoggerFactory.getLogger(ObjectFactoryImpl.class);

    private MsbContextImpl msbContext;
    private PayloadConverter payloadConverter;
    private ChannelMonitorAggregator channelMonitorAggregator;

    public ObjectFactoryImpl(MsbContextImpl msbContext) {
        super();
        this.msbContext = msbContext;
        payloadConverter = new PayloadConverterImpl(msbContext.getPayloadMapper());
    }

    @Override
    public <T extends RestPayload> Requester<T> createRequester(String namespace, RequestOptions requestOptions, TypeReference<T> payloadTypeReference) {
        return RequesterImpl.create(namespace, requestOptions, msbContext, payloadTypeReference);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends RestPayload> ResponderServer<T> createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler, TypeReference<T> payloadTypeReference) {
        return ResponderServerImpl.create(namespace, messageTemplate, msbContext, requestHandler, payloadTypeReference);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PayloadConverter getPayloadConverter() {
        return payloadConverter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized ChannelMonitorAggregator createChannelMonitorAggregator(Callback<AggregatorStats> aggregatorStatsHandler) {
        ThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("monitor-aggregator-heartbeat-thread-%d")
                .daemon(true)
                .build();
        ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1, threadFactory);

        channelMonitorAggregator = createDefaultChannelMonitorAggregator(aggregatorStatsHandler, scheduledExecutorService);
        return channelMonitorAggregator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void shutdown() {
        LOG.info("Shutting down...");
        if (channelMonitorAggregator != null) {
            channelMonitorAggregator.stop();
        }
        LOG.info("Shutdown complete");
    }

    DefaultChannelMonitorAggregator createDefaultChannelMonitorAggregator(Callback<AggregatorStats> aggregatorStatsHandler, ScheduledExecutorService scheduledExecutorService) {
        return new DefaultChannelMonitorAggregator(msbContext, scheduledExecutorService, aggregatorStatsHandler);
    }
}
