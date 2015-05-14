package tcdl.msb;

import java.util.Objects;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import tcdl.msb.config.MsbMessageOptions;
import tcdl.msb.events.Event;
import tcdl.msb.events.TwoArgumentsAdapter;
import tcdl.msb.messages.IncommingMessage;
import tcdl.msb.messages.Message;
import tcdl.msb.messages.MessageFactory;
import tcdl.msb.messages.MetaMessage;
import tcdl.msb.messages.payload.RequestPayload;

/**
 * Created by rdro on 4/27/2015.
 */
public class Requester extends Collector {

    public final static Event ERROR_EVENT = new Event("err");

    private MetaMessage meta;
    private Message message;
   
    MessageFactory messageFactory;

    public Requester(MsbMessageOptions config, IncommingMessage originalMessage) {
        super(config);
        this.messageFactory =MessageFactory.getInstance();
        this.meta = messageFactory.createMeta(config);
        this.message = messageFactory.createRequestMessage(config, originalMessage);
    }

    public void publish(@Nullable RequestPayload payload) {
        if (payload != null) {
            message.withPayload(payload);
        }

        if (isWaitForResponses()) {
            listenForResponses(message.getTopics().getTo(), new Predicate<Message>() {
                public boolean test(Message responsMessage) {                	
                	return Objects.equals(responsMessage.getCorrelationId(), message.getCorrelationId());
                }
            });
        }

        messageFactory.completeMeta(message, meta);

        channelManager.findOrCreateProducer(message.getTopics().getTo())
            .withMessageHandler(new TwoArgumentsAdapter<Message, Exception>() {
                public void onEvent(Message message, Exception exception) {
                    if (exception != null) {
                        emit(ERROR_EVENT, exception);
                        return;
                    }

                    if (!isAwaitingResponses()) end();
                    enableTimeout();
                }})
            .publish(message);
    }
    
    Message getMessage() {
        return message;
    }

}
