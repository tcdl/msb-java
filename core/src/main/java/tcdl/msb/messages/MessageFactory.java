package tcdl.msb.messages;

import java.util.Date;

import org.apache.commons.lang3.Validate;

import tcdl.msb.config.MsbConfigurations;
import tcdl.msb.config.MsbMessageOptions;
import tcdl.msb.ServiceDetails;
import tcdl.msb.messages.payload.RequestPayload;
import tcdl.msb.messages.payload.ResponsePayload;
import tcdl.msb.support.Utils;

import javax.annotation.Nullable;

/**
 * Created by rdro on 4/22/2015.
 */
public class MessageFactory {

    private static MessageFactory INSTANCE = new MessageFactory();
    private ServiceDetails serviceDetails;

    private MessageFactory() {
		tcdl.msb.config.ServiceDetails configServiceDetails = MsbConfigurations.msbConfiguration().getServiceDetails();

		// TODO Get rid of 2 classes ServiceDetails and eliminate this explicit copy
		this.serviceDetails = new ServiceDetails();
		this.serviceDetails.setHostname(configServiceDetails.getHostName());
		this.serviceDetails.setInstanceId(configServiceDetails.getInstanceId());
		this.serviceDetails.setIp(configServiceDetails.getIp());
		this.serviceDetails.setName(configServiceDetails.getName());
		this.serviceDetails.setPid((int) configServiceDetails.getPid());
		this.serviceDetails.setVersion(configServiceDetails.getVersion());
    }

    Message createBaseMessage(@Nullable IncommingMessage originalMessage, boolean isRequestMessage) {
        Message baseMessage = new Message()
				.withId(Utils.generateId())
                .withCorrelationId(
						originalMessage != null && originalMessage.getCorrelationId() != null ? originalMessage
                                .getCorrelationId() : Utils.generateId()).withTopics(new Topics());

        if (isRequestMessage) {
            baseMessage.withPayload(new RequestPayload());
		} else {
            baseMessage.withPayload(new ResponsePayload());
        }

        return baseMessage;
    }

    public Message createRequestMessage(MsbMessageOptions config, IncommingMessage originalMessage) {
		Message message = createBroadcastMessage(config, originalMessage);
        message.getTopics().setResponse(config.getNamespace() + ":response:" + this.serviceDetails.getInstanceId());
        return message;
    }

    private Message createBroadcastMessage(MsbMessageOptions config, IncommingMessage originalMessage) {
        Message message = createBaseMessage(originalMessage,true);
        message.getTopics().setTo(config.getNamespace());
        return message;
    }

    public Message createResponseMessage(Message originalMessage, Acknowledge ack, ResponsePayload payload) {
		Validate.notNull(originalMessage);
        Validate.notNull(originalMessage.getTopics());

        Message message = createBaseMessage(originalMessage, false);
        message.getTopics().setTo(originalMessage.getTopics().getResponse());

        return message.withAck(ack).withPayload(payload);
    }

    public Message createAckMessage(Message msbMessage, Acknowledge ack) {
        Validate.notNull(msbMessage);
        Validate.notNull(msbMessage.getTopics());

        Message message = createBaseMessage(msbMessage, false);
        message.getTopics().setTo(msbMessage.getTopics().getResponse());

        return message.withAck(ack).withPayload(null);
    }

    public Acknowledge createAck(MsbMessageOptions config) {
        return new Acknowledge().withResponderId(Utils.generateId()).withResponsesRemaining(null).withTimeoutMs(null);
    }

    public MetaMessage createMeta(MsbMessageOptions config) {
        return new MetaMessage().withTtl(config.getTtl()).withCreatedAt(new Date()).withDurationMs(null)
                .withServiceDetails(this.serviceDetails);
    }

    public Message completeMeta(Message message, MetaMessage meta) {
        return message.withMeta(meta.withDurationMs(new Date().getTime() - meta.getCreatedAt().getTime()));
    }

    public static MessageFactory getInstance() {
        return INSTANCE;
    }
}
