package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;

/**
 * Provides methods for creation {@link Requester} and {@link ResponderServer}.
 */
public interface ObjectFactory {

    /**
     * @param namespace      topic name to send a request to
     * @param requestOptions options to configure a requester
     * @return instance of a {@link Requester}
     */
    Requester createRequester(String namespace, RequestOptions requestOptions);

    /**
     * @param namespace       topic name to send a request to
     * @param requestOptions  options to configure a requester
     * @param originalMessage original message (to take correlation id from)
     * @return instance of a {@link Requester} with original message
     */
    Requester createRequester(String namespace, RequestOptions requestOptions, Message originalMessage);

    /**
     * @param namespace       topic on a bus for listening on incoming requests
     * @param messageTemplate template used for creating response messages
     * @param requestHandler  handler for processing the request
     * @return new instance of a {@link ResponderServer} that unmarshals payload into default {@link Payload}
     */
    ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler requestHandler);

    /**
     * @param namespace       topic on a bus for listening on incoming requests
     * @param messageTemplate template used for creating response messages
     * @param requestHandler  handler for processing the request
     * @param payloadClass    defines custom payload type
     * @return new instance of a {@link ResponderServer} that unmarshals payload into specified payload type
     */
    ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler requestHandler, Class<? extends Payload> payloadClass);
}