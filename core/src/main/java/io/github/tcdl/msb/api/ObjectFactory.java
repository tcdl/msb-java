package io.github.tcdl.msb.api;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;

/**
 * Provides methods for creation client-facing API classes.
 */
public interface ObjectFactory {

    /**
     * @param namespace      topic name to send a request to
     * @param requestOptions options to configure a requester
     * @return instance of a {@link Requester}
     */
    default Requester<Payload> createRequester(String namespace, RequestOptions requestOptions) {
        return createRequester(namespace, requestOptions, null, Payload.class); // TODO reconsider the set of methods for createRequester
    }

    /**
     * @param namespace       topic name to send a request to
     * @param requestOptions  options to configure a requester
     * @param originalMessage original message (to take correlation id from)
     * @return new instance of a {@link Requester} with original message
     */
    <T extends Payload> Requester<T> createRequester(String namespace, RequestOptions requestOptions, Message originalMessage, Class<T> payloadClass);

    /**
     * @param namespace       topic on a bus for listening on incoming requests
     * @param messageTemplate template used for creating response messages
     * @param requestHandler  handler for processing the request
     * @return new instance of a {@link ResponderServer} that unmarshals payload into default {@link Payload}
     */
    default ResponderServer<Payload> createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<Payload> requestHandler) {
        return createResponderServer(namespace, messageTemplate, requestHandler, Payload.class);
    }

    /**
     * @param namespace       topic on a bus for listening on incoming requests
     * @param messageTemplate template used for creating response messages
     * @param requestHandler  handler for processing the request
     * @param payloadClass    defines custom payload type
     * @return new instance of a {@link ResponderServer} that unmarshals payload into specified payload type
     */
    <T extends Payload> ResponderServer<T> createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler, Class<T> payloadClass);

    /**
     * @return instance of converter to convert any objects
     * using object mapper from {@link MsbContext}
     */
    PayloadConverter getPayloadConverter();

    /**
     * @param aggregatorStatsHandler this handler is invoked whenever statistics is updated via announcement channel or heartbeats.
     *                               THE HANDLER SHOULD BE THREAD SAFE because it may be invoked from parallel threads
     * @return new instance of {@link ChannelMonitorAggregator}
     */
    ChannelMonitorAggregator createChannelMonitorAggregator(Callback<AggregatorStats> aggregatorStatsHandler);

    /**
     * Shuts down the factory and all the objects that were created by it
     */
    void shutdown();
}