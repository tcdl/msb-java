package io.github.tcdl;

import static io.github.tcdl.events.Event.ERROR_EVENT;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.EventEmitter;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Requester is a component which sends a request message to the bus and collects responses
 *
 * Created by rdro on 4/27/2015.
 */
public class Requester {

    public static final Logger LOG = LoggerFactory.getLogger(Requester.class);

    private Collector collector;
    private MessageFactory messageFactory;  
    private MessageBuilder messageBuilder;
    private Message message;
    private String topicToListen;

    /**
     * Creates a new instance of a requester
     * @param config message options to construct a message
     * @param originalMessage original message (to take correlation id from)
     * @param context context which contains MSB related beans
     */
    public Requester(MsbMessageOptions config, Message originalMessage, MsbContext context) {
        Validate.notNull(config, "the 'config' must not be null");
        this.collector = new Collector(config, context.getChannelManager(), context.getMsbConfig());
        this.messageFactory = context.getMessageFactory();       
        this.messageBuilder = messageFactory.createRequestMessageBuilder(config, originalMessage);
    }

    /**
     * Wraps a payload with message meta and sends to the bus
     * @param requestPayload
     */
    public void publish(@Nullable Payload requestPayload) {
        this.message = messageFactory.createRequestMessage(messageBuilder, requestPayload);

        if (collector.isWaitForResponses()) {
            topicToListen = message.getTopics().getResponse();
            collector.listenForResponses(topicToListen,
                    responseMessage -> Objects.equals(responseMessage.getCorrelationId(), message.getCorrelationId())
            );
        }

        collector.getChannelManager().findOrCreateProducer(this.message.getTopics().getTo())
                .publish(this.message, this::handleMessage);
    }

    /**
     * Registers a callback to be called when acknowledge message received
     * @param acknowledgeHandler callback
     * @return requester
     */
    public Requester onAcknowledge(Callback<Acknowledge> acknowledgeHandler) {
        getConsumer().ifPresent(consumer -> consumer.on(Event.ACKNOWLEDGE_EVENT, acknowledgeHandler::call));
        return this;
    }

    /**
     * Registers a callback to be called when response message received
     * @param responseHandler callback
     * @return requester
     */
    public Requester onResponse(Callback<Payload> responseHandler) {
        getConsumer().ifPresent(eventEmitter -> eventEmitter.on(Event.RESPONSE_EVENT, responseHandler::call));
        return this;
    }

    /**
     * Registers a callback to be called when all responses collected
     * @param endHandler callback
     * @return requester
     */
    public Requester onEnd(Callback<List<Message>> endHandler) {
        getConsumer().ifPresent(eventEmitter -> eventEmitter.on(Event.END_EVENT, endHandler::call));
        return this;
    }

    /**
     * Registers a callback to be called if error happened while collecting responses
     * @param errorHandler callback
     * @return requester
     */
    public Requester onError(Callback<Exception> errorHandler) {
        getConsumer().ifPresent(consumer -> consumer.on(Event.ERROR_EVENT, errorHandler::call));
        return this;
    }

    protected void handleMessage(Message message, Exception exception) {
        if (exception != null) {
            getConsumer().ifPresent(consumer -> consumer.emit(ERROR_EVENT, exception));
            LOG.debug("Exception was thrown.", exception);
            return;
        }

        if (!collector.isAwaitingResponses())
            collector.end();
        collector.enableTimeout();
    }

    Message getMessage() {
        return message;
    }
    
    boolean isMessageAcknowledged() {
        return !collector.getAckMessages().isEmpty();
    }

    private Optional<Consumer> getConsumer() {
        return Optional.ofNullable(collector.getChannelManager().findConsumer(topicToListen));
    }
}
