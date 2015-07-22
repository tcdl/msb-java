package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;

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

    @Override public ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate, ResponderServer.RequestHandler requestHandler,
            Class payloadClass) {
        return ResponderServerImpl.create(namespace, messageTemplate, msbContext, requestHandler, payloadClass);
    }
}
