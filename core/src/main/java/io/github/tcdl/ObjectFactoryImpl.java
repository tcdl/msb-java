package io.github.tcdl;

import io.github.tcdl.api.MessageTemplate;
import io.github.tcdl.api.ObjectFactory;
import io.github.tcdl.api.RequestOptions;
import io.github.tcdl.api.Requester;
import io.github.tcdl.api.ResponderServer;
import io.github.tcdl.api.message.Message;
import io.github.tcdl.impl.RequesterImpl;
import io.github.tcdl.impl.ResponderServerImpl;

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
    public ResponderServer createResponderServer(String namespace,  MessageTemplate messageTemplate, MsbContextImpl msbContext, ResponderServer.RequestHandler requestHandler) {
        return ResponderServerImpl.create(namespace, messageTemplate, msbContext, requestHandler);
    }

}
