package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.api.AcknowledgementHandler;
import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.api.ResponderContext;
import io.github.tcdl.msb.api.message.Message;

/**
 * Implementation of {@link ResponderContext} Provide access to {@link Responder} 
 * that used for sending response and {@link AcknowledgementHandler} that used
 * for explicit confirm/reject received request
 */
public class ResponderContextImpl implements ResponderContext {

    private final Responder responder;
    private final AcknowledgementHandler acknowledgementHandler;
    private final Message originalMessage;

    public ResponderContextImpl(Responder responder, AcknowledgementHandler acknowledgementHandler, Message originalMessage) {
        super();
        this.responder = responder;
        this.acknowledgementHandler = acknowledgementHandler;
        this.originalMessage = originalMessage;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Responder getResponder() {
        return responder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AcknowledgementHandler getAcknowledgementHandler() {
        return acknowledgementHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message getOriginalMessage() {
        return originalMessage;
    }
    
}
