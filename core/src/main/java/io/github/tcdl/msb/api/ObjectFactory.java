package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;

/**
 * Provides methods for creation client-facing API classes.
 */
public interface ObjectFactory {
    /**
     * Creates a new instance of {@link Requester}.
     *
     * @param namespace      topic name to send a request to
     * @param requestOptions options to configure a requester
     * @return instance of a requester
     */
    Requester createRequester(String namespace, RequestOptions requestOptions);

    /**
     * Creates a new instance of {@link Requester} with originalMessage.
     *
     * @param namespace       topic name to send a request to
     * @param requestOptions  options to configure a requester
     * @param originalMessage original message (to take correlation id from)
     * @return instance of a requester
     */
    Requester createRequester(String namespace, RequestOptions requestOptions, Message originalMessage);

    /**
     * Create a new instance of {@link ResponderServer}.
     *
     * @param namespace       topic on a bus for listening on incoming requests
     * @param messageTemplate template used for creating response messages
     * @param requestHandler  handler for processing the request
     * @return new instance of a {@link ResponderServer}
     */
    ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate, 
            ResponderServer.RequestHandler requestHandler);

    /**
     * @param aggregatorStatsHandler this handler is invoked whenever statistics is updated via announcement channel or heartbeats.
     *                               THE HANDLER SHOULD BE THREAD SAFE because it may be invoked from parallel threads
     * @return new instance of {@link ChannelMonitorAggregator}
     */
    ChannelMonitorAggregator createChannelMonitorAggregator(Callback<AggregatorStats> aggregatorStatsHandler);
}