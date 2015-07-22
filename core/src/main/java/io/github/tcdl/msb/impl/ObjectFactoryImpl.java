package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;
import io.github.tcdl.msb.monitor.aggregator.DefaultChannelMonitorAggregator;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

/**
 * Provides methods for creation {@link Requester} and {@link ResponderServer}.
 */
public class ObjectFactoryImpl implements ObjectFactory {
    private MsbContextImpl msbContext;
    
    public ObjectFactoryImpl(MsbContextImpl msbContext) {
        super();
        this.msbContext = msbContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Requester createRequester(String namespace, RequestOptions requestOptions) {
        return RequesterImpl.create(namespace, requestOptions, msbContext);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Requester createRequester(String namespace, RequestOptions requestOptions, Message originalMessage) {
        return RequesterImpl.create(namespace, requestOptions, originalMessage, msbContext);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResponderServer createResponderServer(String namespace,  MessageTemplate messageTemplate, ResponderServer.RequestHandler requestHandler) {
        return ResponderServerImpl.create(namespace, messageTemplate, msbContext, requestHandler, Payload.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override public ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate, ResponderServer.RequestHandler requestHandler,
            Class<? extends Payload> payloadClass) {
        return ResponderServerImpl.create(namespace, messageTemplate, msbContext, requestHandler, payloadClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ChannelMonitorAggregator createChannelMonitorAggregator(Callback<AggregatorStats> aggregatorStatsHandler) {
        ThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("monitor-aggregator-heartbeat-thread-%d")
                .daemon(true)
                .build();
        ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1, threadFactory);

        return new DefaultChannelMonitorAggregator(msbContext.getChannelManager(), msbContext.getObjectFactory(), msbContext.getMessageMapper(), scheduledExecutorService, aggregatorStatsHandler);
    }
}
