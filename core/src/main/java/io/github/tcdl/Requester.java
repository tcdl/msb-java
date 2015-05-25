package io.github.tcdl;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.TwoArgsEventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.MetaMessage.MetaMessageBuilder;
import io.github.tcdl.messages.payload.Payload;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;

import static io.github.tcdl.events.Event.ERROR_EVENT;

/**
 * Created by rdro on 4/27/2015.
 */
public class Requester extends Collector {

    public static final Logger LOG = LoggerFactory.getLogger(Requester.class);

    private MessageFactory messageFactory;
    private Message message;
    private MetaMessageBuilder metaBuilder;
    private MessageBuilder messageBuilder;
    private ChannelManager channelManager;

    public Requester(MsbMessageOptions config, Message originalMessage, MessageFactory messageFactory, ChannelManager channelManager, MsbConfigurations msbConfigurations) {
        super(config, channelManager, msbConfigurations);
        this.channelManager = channelManager;
        Validate.notNull(config, "the 'config' must not be null");
        this.messageFactory = messageFactory;
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
        
        TwoArgsEventHandler<Message, Exception> callback = (message, exception) -> {
            if (exception != null) {
                emit(ERROR_EVENT, exception);
                LOG.debug("Exception was thrown.", exception);
                return;
            }

            if (!isAwaitingResponses())
                end();
            enableTimeout();
        };
        
        channelManager.findOrCreateProducer(this.message.getTopics().getTo())
                .publish(this.message, callback);
    }

    Message getMessage() {
        return message;
    }
}
