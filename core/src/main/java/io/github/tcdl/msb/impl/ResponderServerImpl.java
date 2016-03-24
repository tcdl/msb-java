package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.AcknowledgementHandler;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.api.ResponderContext;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ResponderServerImpl<T> implements ResponderServer {
    private static final Logger LOG = LoggerFactory.getLogger(ResponderServerImpl.class);

    private String namespace;
    private MsbContextImpl msbContext;
    private MessageTemplate messageTemplate;
    private RequestHandler<T> requestHandler;
    private Optional<ErrorHandler> errorHandler;
    private ObjectMapper payloadMapper;
    private TypeReference<T> payloadTypeReference;

    private ResponderServerImpl(String namespace,
            MessageTemplate messageTemplate,
            MsbContextImpl msbContext,
            RequestHandler<T> requestHandler,
            ErrorHandler errorHandler,
            TypeReference<T> payloadTypeReference) {
        this.namespace = namespace;
        this.messageTemplate = messageTemplate;
        this.msbContext = msbContext;
        this.requestHandler = requestHandler;
        this.errorHandler = Optional.ofNullable(errorHandler);
        this.payloadMapper = msbContext.getPayloadMapper();
        this.payloadTypeReference = payloadTypeReference;
        Validate.notNull(requestHandler, "requestHandler must not be null");
    }

    /**
     * {@link io.github.tcdl.msb.api.ObjectFactory#createResponderServer(String, MessageTemplate, RequestHandler, ErrorHandler, Class)}
     */
    static <T> ResponderServerImpl<T> create(String namespace,  MessageTemplate messageTemplate, MsbContextImpl msbContext,
            RequestHandler<T> requestHandler,  ErrorHandler errorHandler, TypeReference<T> payloadTypeReference) {
        return new ResponderServerImpl<>(namespace, messageTemplate, msbContext, requestHandler, errorHandler, payloadTypeReference);
    }

    /**
     * Start listening for message on specified topic.
     *
     * If exception was thrown during handling business logic response message with statusCode={@link #INTERNAL_SERVER_ERROR_CODE}
     * and statusMessage=${exception.getMessage()} will be created and sent automatically.
     * If error occurred during message payload conversion to specified type response message with statusCode={@link #PAYLOAD_CONVERSION_ERROR_CODE }
     * and statusMessage="Failed to convert object [${jsonPayload}] to type T}" will be created and sent automatically.
     */
    @Override
    public ResponderServer listen() {
        ChannelManager channelManager = msbContext.getChannelManager();

        channelManager.subscribe(namespace,
                (incomingMessage, acknowledgeHandler) -> {
                    LOG.debug("[{}] Received message with id: [{}]", namespace, incomingMessage.getId());
                    Responder responder = createResponder(incomingMessage);
                    ResponderContext responderContext = createResponderContext(responder, acknowledgeHandler, incomingMessage);
                    onResponder(responderContext);
                });

        return this;
    }

    Responder createResponder(Message incomingMessage) {
        if (isResponseNeeded(incomingMessage)) {
            return new ResponderImpl(messageTemplate, incomingMessage, msbContext);
        } else {
            return new NoopResponderImpl(incomingMessage);
        }
    }

    ResponderContext createResponderContext(Responder responder, AcknowledgementHandler acknowledgeHandler, Message incomingMessage) {
        return new ResponderContextImpl(responder, acknowledgeHandler, incomingMessage);
    }

    void onResponder(ResponderContext responderContext) {
        Message originalMessage = responderContext.getOriginalMessage();
        Object rawPayload = originalMessage.getRawPayload();
        try {
            T request = Utils.convert(rawPayload, payloadTypeReference, payloadMapper);
            LOG.debug("[{}] Process message with id: [{}]", namespace, originalMessage.getId());
            requestHandler.process(request, responderContext);
        } catch (JsonConversionException conversionEx) {
            errorHandler(responderContext, conversionEx, PAYLOAD_CONVERSION_ERROR_CODE);
            if (errorHandler.isPresent()) {
                errorHandler.get().handle(conversionEx, originalMessage);
            }
        } catch (Exception internalEx) {
            errorHandler(responderContext, internalEx, INTERNAL_SERVER_ERROR_CODE);
            if (errorHandler.isPresent()) {
                errorHandler.get().handle(internalEx, originalMessage);
            }
        }
    }

    private boolean isResponseNeeded(Message incomingMessage) {
        return incomingMessage.getTopics().getResponse() != null;
    }

    private void errorHandler(ResponderContext responderContext, Exception exception, int errorStatusCode) {
        Message originalMessage = responderContext.getOriginalMessage();
        LOG.error("[{}] Error while processing message with id: [{}]", namespace, originalMessage.getId(), exception);
        RestPayload responsePayload = new RestPayload.Builder()
                .withStatusCode(errorStatusCode)
                .withStatusMessage(exception.getMessage())
                .build();
        responderContext.getResponder().send(responsePayload);
        //Confirm message for prevention requeue message with incorrect structure
        responderContext.getAcknowledgementHandler().confirmMessage();
    }
}