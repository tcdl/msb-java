package io.github.tcdl;

import io.github.tcdl.api.ObjectFactory;
import io.github.tcdl.api.RequestOptions;
import io.github.tcdl.api.Requester;
import io.github.tcdl.api.message.Message;

/**
 * Provides methods for creation Requester and ResponderServer
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
}
