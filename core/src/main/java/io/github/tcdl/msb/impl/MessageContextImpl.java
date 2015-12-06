package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.api.AcknowledgementHandler;
import io.github.tcdl.msb.api.MessageContext;
import io.github.tcdl.msb.api.message.Message;

public class MessageContextImpl implements MessageContext {
    private final Message originalMessage;
    private final AcknowledgementHandler acknowledgementHandler;

    public MessageContextImpl(AcknowledgementHandler acknowledgementHandler, Message originalMessage) {
        super();
        this.acknowledgementHandler = acknowledgementHandler;
        this.originalMessage = originalMessage;
    }

    @Override
    public AcknowledgementHandler getAcknowledgementHandler() {
        return acknowledgementHandler;
    }

    @Override
    public Message getOriginalMessage() {
        return originalMessage;
    }

}
