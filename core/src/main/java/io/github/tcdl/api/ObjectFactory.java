package io.github.tcdl.api;

import io.github.tcdl.api.message.Message;

/**
 * Create objects which start message communications through message bus 
 */
public interface ObjectFactory {
    /**
     * Creates a new instance of a requester.
     * @param namespace topic name to send a request to
     * @param requestOptions options to configure a requester
     * @return instance of a requester
     */
    public Requester createRequester(String namespace, RequestOptions requestOptions);

    /**
     * Creates a new instance of a requester with originalMessage.
     * @param namespace topic name to send a request to
     * @param requestOptions options to configure a requester
     * @param originalMessage original message (to take correlation id from)
     * @return instance of a requester
     */
    public Requester createRequester(String namespace, RequestOptions requestOptions, Message originalMessage);
    
}
