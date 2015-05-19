package io.github.tcdl;

import java.util.Objects;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.tcdl.Requester;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MetaMessage.MetaMessageBuilder;
import io.github.tcdl.messages.payload.Payload;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.TwoArgumentsAdapter;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MessageFactory;

/**
 * Created by rdro on 4/27/2015.
 */
public class Requester extends Collector {

    public static final Logger LOG = LoggerFactory.getLogger(Requester.class);
    public static final Event ERROR_EVENT = new Event("err");

    private MessageFactory messageFactory;
    private Message message;
    private MetaMessageBuilder metaBuilder;
    private MessageBuilder messageBuilder;

    public Requester(MsbMessageOptions config, Message originalMessage) {
        super(config);
        Validate.notNull(config, "the 'config' must not be null");
        this.messageFactory = MessageFactory.getInstance();
        this.metaBuilder = messageFactory.createMeta(config);
        this.messageBuilder = messageFactory.createRequestMessage(config, originalMessage);
    }

    public void publish(@Nullable Payload requestPayload) {
        if (requestPayload != null) {
            messageBuilder.setPayload(requestPayload);
        }        
        this.message = messageFactory.completeMeta(messageBuilder, metaBuilder);

        if (isWaitForResponses()) {
            listenForResponses(message.getTopics().getResponse(), (responseMessage) ->
                            Objects.equals(responseMessage.getCorrelationId(), message.getCorrelationId())
            );
        }
        
        channelManager.findOrCreateProducer(this.message.getTopics().getTo())
                .withMessageHandler(new TwoArgumentsAdapter<Message, Exception>() {
                    public void onEvent(Message message, Exception exception) {
                        if (exception != null) {
                            emit(ERROR_EVENT, exception);
                            LOG.debug("Exception was thrown.", exception);
                            return;
                        }

                        if (!isAwaitingResponses())
                            end();
                        enableTimeout();
                    }
                })
                .publish(this.message);
    }

    Message getMessage() {
        return message;
    }
}
