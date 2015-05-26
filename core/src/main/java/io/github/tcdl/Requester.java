package io.github.tcdl;

import static io.github.tcdl.events.Event.ERROR_EVENT;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.*;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.MetaMessage.MetaMessageBuilder;
import io.github.tcdl.messages.payload.Payload;

import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rdro on 4/27/2015.
 */
public class Requester {

    public static final Logger LOG = LoggerFactory.getLogger(Requester.class);

    private Collector collector;
    private MessageFactory messageFactory;
    private Message message;
    private MetaMessageBuilder metaBuilder;
    private MessageBuilder messageBuilder;

    public Requester(MsbMessageOptions config, Message originalMessage) {
        Validate.notNull(config, "the 'config' must not be null");
        this.collector = new Collector(config);
        this.messageFactory = getMessageFactory();
        this.metaBuilder = messageFactory.createMeta(config);
        this.messageBuilder = messageFactory.createRequestMessage(config, originalMessage);
    }

    public void publish(@Nullable Payload requestPayload) {
        if (requestPayload != null) {
            messageBuilder.setPayload(requestPayload);
        }        
        this.message = messageFactory.completeMeta(messageBuilder, metaBuilder);

        if (collector.isWaitForResponses()) {
            collector.listenForResponses(message.getTopics().getResponse(), (responseMessage) ->
                            Objects.equals(responseMessage.getCorrelationId(), message.getCorrelationId())
            );
        }
        
        TwoArgsEventHandler<Message, Exception> callback = (message, exception) -> {
            if (exception != null) {
                collector.getChannelManager().emit(ERROR_EVENT, exception);
                LOG.debug("Exception was thrown.", exception);
                return;
            }

            if (!collector.isAwaitingResponses())
                collector.end();
            collector.enableTimeout();
        };

        collector.getChannelManager().findOrCreateProducer(this.message.getTopics().getTo())
                .publish(this.message, callback);
    }

    public Requester onAcknowledge(SingleArgEventHandler<Acknowledge> acknowledgeHandler) {
        collector.getChannelManager().on(Event.ACKNOWLEDGE_EVENT, acknowledgeHandler);
        return this;
    }

    public Requester onPayload(SingleArgEventHandler<Payload> payloadHandler) {
        collector.getChannelManager().on(Event.PAYLOAD_EVENT, payloadHandler);
        return this;
    }

    public Requester onResponse(SingleArgEventHandler<Payload> responseHandler) {
        collector.getChannelManager().on(Event.RESPONDER_EVENT, responseHandler);
        return this;
    }

    public Requester onEnd(GenericEventHandler endHandler) {
        collector.getChannelManager().on(Event.END_EVENT, endHandler);
        return this;
    }

    public Requester onError(SingleArgEventHandler<Exception> errorHandler) {
        collector.getChannelManager().on(Event.ERROR_EVENT, errorHandler);
        return this;
    }

    Message getMessage() {
        return message;
    }
    
    MessageFactory getMessageFactory() {
        return new MessageFactory();
    }

    boolean isMessageAcknowledged() {
        return !collector.getAckMessages().isEmpty();
    }
}
