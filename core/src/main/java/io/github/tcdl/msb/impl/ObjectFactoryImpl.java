package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.monitor.aggregator.DefaultChannelMonitorAggregator;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Collections;
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
    public <T> Requester<T> createRequester(String namespace, RequestOptions requestOptions, TypeReference<T> payloadTypeReference) {
        return RequesterImpl.create(namespace, requestOptions, msbContext, payloadTypeReference);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Requester<T> createRequesterForSingleResponse(String namespace, Class<T> payloadClass, RequestOptions baseRequestOptions) {
        Validate.notNull(baseRequestOptions);
        RequestOptions singleResponseRequestOptions = baseRequestOptions
                .asBuilder()
                .withWaitForResponses(1)
                .build();

        return RequesterImpl.create(namespace, singleResponseRequestOptions, msbContext, toTypeReference(payloadClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> ResponderServer createResponderServer(String namespace, ResponderOptions responderOptions,
                                                     ResponderServer.RequestHandler<T> requestHandler, ResponderServer.ErrorHandler errorHandler, TypeReference<T> payloadTypeReference) {
        return ResponderServerImpl.create(namespace, responderOptions, msbContext, requestHandler, errorHandler, payloadTypeReference);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Requester<T> createRequesterForFireAndForget(String namespace, RequestOptions requestOptions) {
        Validate.notNull(requestOptions, "RequestOptions are mandatory");

        RequestOptions fireAndForgetRequestOptions = requestOptions
                .asBuilder()
                .withAckTimeout(0)
                .withWaitForResponses(0)
                .build();

        return RequesterImpl.create(namespace, fireAndForgetRequestOptions, msbContext, null);
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

    private static <U> TypeReference<U> toTypeReference(Class<U> clazz) {
        return new TypeReference<U>() {
            @Override
            public Type getType() {
                return clazz;
            }
        };
    }
}
