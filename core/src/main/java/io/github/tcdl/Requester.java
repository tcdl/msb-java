package io.github.tcdl;

import io.github.tcdl.config.RequestOptions;
import io.github.tcdl.events.EventHandlers;
import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;
import org.apache.commons.lang3.Validate;

import javax.annotation.Nullable;
import java.util.List;

/**
 * {@link Requester} enable user send message to bus and process responses for this messages if any expected.
 *
 * Expected responses are matched by correlationId from original request.
 *
 * Expected number of messages and response timeouts is defined by {@link RequestOptions} during instance creation but can be updated by so called Acknowledgement
 * response mechanism in case we create Requester with {@literal RequestOptions.waitForResponses => 0} and received Acknowledgement response
 * before RequestOptions.ackTimeout or RequestOptions.responseTimeout (takes max of two).
 *
 * Please note: RequestOptions.waitForResponses represent number of response messages  with {@link io.github.tcdl.messages.payload.Payload} set and in case we received
 * all expected before RequestOptions.responseTimeout we don't wait for Acknowledgement response and RequestOptions.ackTimeout is not used.
 */
public class Requester {

    private RequestOptions requestOptions;
    private MsbContext context;

    private Message message;
    private MessageFactory messageFactory;
    private MessageBuilder messageBuilder;
    EventHandlers eventHandlers;

    /**
     * Creates a new instance of a requester.
     *
     * @param namespace topic name to send a request to
     * @param requestOptions options to configure a requester
     * @param context shared by all Requester instances
     * @return instance of a requester
     */
    public static Requester create(String namespace, RequestOptions requestOptions, MsbContext context) {
        return new Requester(namespace, requestOptions, null, context);
    }

    /**
     * Creates a new instance of a requester with originalMessage.
     *
     * @param namespace topic name to send a request to
     * @param requestOptions options to configure a requester
     * @param originalMessage original message (to take correlation id from)
     * @param context shared by all Requester instances
     * @return instance of a requester
     */
    public static Requester create(String namespace, RequestOptions requestOptions, Message originalMessage, MsbContext context) {
        return new Requester(namespace, requestOptions, originalMessage, context);
    }

    private Requester(String namespace, RequestOptions requestOptions, Message originalMessage, MsbContext context) {
        Validate.notNull(namespace, "the 'namespace' must not be null");
        Validate.notNull(requestOptions, "the 'messageOptions' must not be null");
        Validate.notNull(context, "the 'context' must not be null");

        this.requestOptions = requestOptions;
        this.context = context;

        this.eventHandlers = new EventHandlers();
        this.messageFactory = context.getMessageFactory();
        this.messageBuilder = messageFactory.createRequestMessageBuilder(namespace, requestOptions.getMessageTemplate(), originalMessage);
    }

    /**
     * Wraps a payload with protocol information and sends to bus.
     * In case Requester created with expectation for responses then process them.
     *
     * @param requestPayload payload which will be sent to bus
     * @throws ChannelException if an error is encountered during publishing to bus
     * @throws JsonConversionException if unable to parse message to JSON before sending to bus
     */
    public void publish(@Nullable Payload requestPayload) {
        this.message = messageFactory.createRequestMessage(messageBuilder, requestPayload);

        //use Collector instance to handle expected responses/acks
        if (requestOptions.isWaitForResponses()) {
            Collector collector = createCollector(requestOptions, context, eventHandlers);
            String topic = message.getTopics().getResponse();
            collector.listenForResponses(topic, this.message);

            getChannelManager().findOrCreateProducer(this.message.getTopics().getTo())
                    .publish(this.message);

            collector.waitForResponses();
        } else {
            getChannelManager().findOrCreateProducer(this.message.getTopics().getTo())
                    .publish(this.message);
        }
    }

    /**
     * Registers a callback to be called when {@link Message} with {@link Acknowledge} property set is received.
     *
     * @param acknowledgeHandler callback to be called
     * @return requester
     */
    public Requester onAcknowledge(Callback<Acknowledge> acknowledgeHandler) {
        eventHandlers.onAcknowledge(acknowledgeHandler);
        return this;
    }

    /**
     * Registers a callback to be called when response {@link Message} with {@link Payload} property set is received.
     *
     * @param responseHandler callback to be called
     * @return requester
     */
    public Requester onResponse(Callback<Payload> responseHandler) {
        eventHandlers.onResponse(responseHandler);
        return this;
    }

    /**
     * Registers a callback to be called when all expected responses for request message are processes or awaiting timeout for responses occurred.
     *
     * @param endHandler callback to be called
     * @return requester
     */
    public Requester onEnd(Callback<List<Message>> endHandler) {
        eventHandlers.onEnd(endHandler);
        return this;
    }

    protected Message getMessage() {
        return message;
    }

    private ChannelManager getChannelManager() {
        return context.getChannelManager();
    }

    Collector createCollector(RequestOptions requestOptions, MsbContext context, EventHandlers eventHandlers) {
        return new Collector(requestOptions, context, eventHandlers);
    }
}
