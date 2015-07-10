package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.message.Message;

/**
 * Provides methods for creation {@link Requester} and {@link ResponderServer}.
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
}