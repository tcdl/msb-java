package io.github.tcdl.msb.collector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.api.AcknowledgementHandler;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageContext;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.events.EventHandlers;
import io.github.tcdl.msb.impl.MessageContextImpl;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.support.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

import static io.github.tcdl.msb.support.Utils.ifNull;
import static java.lang.Math.toIntExact;

/**
 * {@link Collector} is a component which collects responses and acknowledgements for sent requests.
 */
public class Collector<T> implements ConsumedMessagesAwareMessageHandler, ExecutionOptionsAwareMessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(Collector.class);

    private final List<Message> ackMessages;
    private final List<Message> payloadMessages;

    private final Map<String, Integer> timeoutMsById;
    private final Map<String, Integer> responsesRemainingById;
    private final Set<String> handledMessagesIds;

    private final int timeoutMs;
    private volatile int currentTimeoutMs;
    private final Integer waitForAcksMs;
    private volatile Instant waitForAcksUntil;

    private volatile int responsesRemaining;
    private final boolean shouldWaitUntilResponseTimeout;

    private final TypeReference<T> payloadTypeReference;

    private final long startedAt;
    private final TimeoutManager timeoutManager;
    private final ObjectMapper payloadMapper;

    private final Clock clock;
    private final Message requestMessage;

    private final Optional<BiConsumer<Message, MessageContext>> onRawResponse;
    private final Optional<BiConsumer<T, MessageContext>> onResponse;
    private final Optional<BiConsumer<Acknowledge, MessageContext>> onAcknowledge;
    private final Optional<Callback<Void>> onEnd;
    private final Optional<BiConsumer<Exception, Message>> onError;

    private ScheduledFuture ackTimeoutFuture;
    private ScheduledFuture responseTimeoutFuture;
    private final CollectorManager collectorManager;

    /**
     * Count of consumed incoming messages so {@link #handleMessage} invocation is expected in future.
     * Even redelivered messages increment this counter.
     */
    private final LongAdder consumedMessagesCount = new LongAdder();

    /**
     * Count of consumed incoming messages that were lost afterwards so {@link #handleMessage} invocation is
     * no longer expected
     */
    private final LongAdder consumedAndLostMessagesCount = new LongAdder();

    /**
     * Counter of consumed incoming messages that are already handled by {@link #handleMessage}.
     */
    private final LongAdder handledMessagesHandledCount = new LongAdder();

    /**
     * Is the current instance unsubscribed from message source so new incoming messages are no longer expected.
     */
    private volatile boolean isUnsubscribed = false;

    /**
     * Was the "onEnd" callback invoked? Used to guarantee that "onEnd" will not be invoked more than once.
     */
    private volatile boolean isOnEndInvoked = false;

    private final boolean directlyInvokable;

    public Collector(String topic, Message requestMessage, RequestOptions requestOptions, MsbContextImpl msbContext, EventHandlers<T> eventHandlers,
            TypeReference<T> payloadTypeReference) {
        this(topic, requestMessage, requestOptions, msbContext, eventHandlers, payloadTypeReference, false);
    }

    public Collector(String topic, Message requestMessage, RequestOptions requestOptions, MsbContextImpl msbContext, EventHandlers<T> eventHandlers,
                     TypeReference<T> payloadTypeReference, boolean directlyInvokableCallbacks) {
        this.requestMessage = requestMessage;

        this.clock = msbContext.getClock();
        this.collectorManager = msbContext.getCollectorManagerFactory().findOrCreateCollectorManager(topic);
        this.timeoutManager = msbContext.getTimeoutManager();
        this.payloadMapper = msbContext.getPayloadMapper();

        this.startedAt = clock.instant().toEpochMilli();
        this.ackMessages = new LinkedList<>();
        this.payloadMessages = new LinkedList<>();
        this.timeoutMsById = new HashMap<>();
        this.responsesRemainingById = new HashMap<>();
        this.handledMessagesIds = new HashSet<>();

        this.waitForAcksMs = requestOptions.getAckTimeout();
        this.waitForAcksUntil = null;

        this.timeoutMs = getResponseTimeoutFromConfigs(requestOptions);
        this.currentTimeoutMs = timeoutMs;

        this.responsesRemaining = requestOptions.getWaitForResponses();

        this.shouldWaitUntilResponseTimeout = (responsesRemaining == RequestOptions.WAIT_FOR_RESPONSES_UNTIL_TIMEOUT);

        this.payloadTypeReference = payloadTypeReference;

        onRawResponse = Optional.ofNullable(eventHandlers.onRawResponse());
        onResponse = Optional.ofNullable(eventHandlers.onResponse());
        onAcknowledge = Optional.ofNullable(eventHandlers.onAcknowledge());
        onEnd = Optional.ofNullable(eventHandlers.onEnd());
        onError = Optional.ofNullable(eventHandlers.onError());
        this.directlyInvokable = directlyInvokableCallbacks;
    }

    @Override
    public synchronized void notifyMessageConsumed() {
        consumedMessagesCount.increment();
    }

    @Override
    public synchronized void notifyConsumedMessageIsLost() {
        consumedAndLostMessagesCount.increment();
        if(isNoMoreMessagesHandlingPossible()) {
            end();
        }
    }

    boolean isAwaitingAcks() {
        return this.waitForAcksUntil != null && waitForAcksUntil.isAfter(Instant.now());
    }

    boolean isAwaitingResponses() {
        return shouldWaitUntilResponseTimeout || getResponsesRemaining() > 0;
    }

    public void listenForResponses() {
        if (this.waitForAcksMs != null && this.waitForAcksMs != 0) {
            this.waitForAcksUntil = Instant.ofEpochMilli(this.startedAt).plusMillis(waitForAcksMs);
        }
        collectorManager.registerCollector(this);
    }

    @Override
    public void handleMessage(Message incomingMessage, AcknowledgementHandler acknowledgeHandler) {
        LOG.debug("[correlation ids: {}-{}] Received {}",
                requestMessage.getCorrelationId(), incomingMessage.getCorrelationId(), incomingMessage);

        JsonNode rawPayload = incomingMessage.getRawPayload();
        MessageContext messageContext = createMessageContext(acknowledgeHandler, incomingMessage);
        boolean isWithPayload = Utils.isPayloadPresent(rawPayload);

        if (isWithPayload) {
            LOG.debug("[correlation ids: {}-{}] Received Payload {}",
                    requestMessage.getCorrelationId(), incomingMessage.getCorrelationId(), rawPayload);
            payloadMessages.add(incomingMessage);
            try {
                onRawResponse.ifPresent(handler -> handler.accept(incomingMessage, messageContext));

                T payload = Utils.convert(rawPayload, payloadTypeReference, payloadMapper);
                onResponse.ifPresent(handler -> handler.accept(payload, messageContext));
            } catch (Exception e) {
                //do not propagate exception outside of this method in order to prevent autoRetry for responses
                LOG.warn("Unexpected exception during response handler invocation", e);
                onError.ifPresent(handler -> handler.accept(e, incomingMessage));
            }
        } else {
            LOG.debug("[correlation ids: {}-{}] Received {}",
                    requestMessage.getCorrelationId(), incomingMessage.getCorrelationId(), incomingMessage.getAck());
            ackMessages.add(incomingMessage);
            onAcknowledge.ifPresent(handler -> handler.accept(incomingMessage.getAck(), messageContext));
        }

        processAck(incomingMessage.getAck());

        synchronized (this) {
            handledMessagesHandledCount.increment();
            updateCounters(incomingMessage, isWithPayload);

            boolean isInvokeOnEnd = false;
            if (!isAwaitingResponses()) {
                //set ack timer task in case we received ALL expected responses but still have to wait for ack
                if (isAwaitingAcks()) {
                    waitForAcks();
                } else {
                    isInvokeOnEnd = true;
                }
            }

            isInvokeOnEnd = isInvokeOnEnd || isNoMoreMessagesHandlingPossible();

            if(isInvokeOnEnd) {
                LOG.debug("[correlation ids: {}] All messages has been received", requestMessage.getCorrelationId());
                end();
            }
        }
    }

    MessageContext createMessageContext(AcknowledgementHandler acknowledgementHandler, Message originalMessage) {
        return new MessageContextImpl(acknowledgementHandler, originalMessage);
    }

    protected synchronized void end() {
        LOG.debug("[correlation id: {}] Stop response processing ", requestMessage.getCorrelationId());
        cancelAckTimeoutTask();
        cancelResponseTimeoutTask();

        collectorManager.unregisterCollector(this);
        isUnsubscribed = true;

        if(!isOnEndInvoked && isAllConsumedMessagesHandled()) {
            isOnEndInvoked = true;
            LOG.debug("[correlation id: {}] triggering 'onEnd' callback", requestMessage.getCorrelationId());
            try {
                onEnd.ifPresent(handler -> handler.call(null));
            } catch (Exception e) {
                LOG.warn("Unexpected exception during 'onEnd' handler invocation", e);
            }
        }
    }

    /**
     * Returns true if no more {@link #handleMessage} invocations are expected.
     */
    private boolean isNoMoreMessagesHandlingPossible() {
        return isUnsubscribed && isAllConsumedMessagesHandled();
    }

    /**
     * Returns true if all incoming messages consumed at the moment were handled by {@link #handleMessage}.
     * But it is possible, that some new messages will be consumed and handled afterwards.
     */
    private synchronized boolean isAllConsumedMessagesHandled() {
        int consumed = consumedMessagesCount.intValue();
        int handled = handledMessagesHandledCount.intValue();
        int consumedAndLost = consumedAndLostMessagesCount.intValue();

        LOG.debug("[correlation id: {}] Messages consumed: {}; handled: {} consumed and lost: {}  ",
                requestMessage.getCorrelationId(), consumed, handled, consumedAndLost);

        return (consumed == consumedAndLost + handled);
    }

    void processAck(Acknowledge acknowledge) {
        if (acknowledge == null)
            return;

        if (acknowledge.getResponsesRemaining() != null) {
            LOG.debug("[correlation id: {}] Responses remaining for responderId [{}] is set to {}",
                    requestMessage.getCorrelationId(), acknowledge.getResponderId(),
                    setResponsesRemainingForResponderId(acknowledge.getResponderId(), acknowledge.getResponsesRemaining()));
        }

        if (acknowledge.getTimeoutMs() != null) {
            setTimeoutMsForResponderId(acknowledge.getResponderId(), acknowledge.getTimeoutMs());
        }

        Integer newTimeoutMs = getMaxTimeoutMs();
        if (newTimeoutMs != this.currentTimeoutMs) {
            this.currentTimeoutMs = newTimeoutMs;
            waitForResponses();
        }
    }

    private Integer setTimeoutMsForResponderId(String responderId, Integer timeoutMs) {
        if (timeoutMsById != null && timeoutMsById.containsKey(responderId) && timeoutMsById.get(responderId).equals(timeoutMs)) {
            return 0; // Not changed
        }
        timeoutMsById.put(responderId, timeoutMs);

        return timeoutMs;
    }

    private int getMaxTimeoutMs() {
        if (timeoutMsById.isEmpty()) {
            return this.timeoutMs;
        }

        Integer maxTimeoutMs = this.timeoutMs;
        for (String responderId : timeoutMsById.keySet()) {
            // Use only what we're waiting for
            if (!responsesRemainingById.isEmpty() && responsesRemainingById.containsKey(responderId)
                    && responsesRemainingById.get(responderId) == 0)
                continue;
            maxTimeoutMs = Math.max(timeoutMsById.get(responderId), maxTimeoutMs);
        }

        return maxTimeoutMs;
    }

    private synchronized void updateCounters(Message message, boolean isWithPayload) {
        String id = message.getId();

        /**
         * Don't update remaining messages counter when a message id was already recorder so the current
         * message is a redelivery of a previous one.
         */
        if(!handledMessagesIds.contains(id)) {
            if(isWithPayload) {
                incResponsesRemaining(-1);
            }
            handledMessagesIds.add(message.getId());
        }
    }

    private synchronized Integer incResponsesRemaining(Integer inc) {
        return (responsesRemaining = Math.max(responsesRemaining + inc, 0));
    }

    int getResponsesRemaining() {
        if (responsesRemainingById.isEmpty()) {
            return responsesRemaining;
        }

        Integer sumOfResponsesRemaining = 0;
        for (Integer responses : responsesRemainingById.values()) {
            sumOfResponsesRemaining += responses;
        }

        return Math.max(responsesRemaining, sumOfResponsesRemaining);
    }

    private Integer setResponsesRemainingForResponderId(String responderId, int responsesRemaining) {
        //check for responsesRemaining < 0 seems redundant, since if config.waitForResponses == -1 we use  Infinity
        boolean atMin = (responsesRemaining < 0 && (responsesRemainingById.isEmpty() || !responsesRemainingById
                .containsKey(responderId)));
        if (atMin) {
            return null;
        }
        //when second, third, etc time same value (not equals 0) for responsesRemaining is received for corresponding responderId, it must be sum up with previous.
        if (responsesRemaining == 0) {
            responsesRemainingById.put(responderId, 0);
        } else {
            responsesRemainingById.put(responderId,
                    Math.max(0, ifNull(responsesRemainingById.get(responderId), 0) + responsesRemaining));
        }

        return responsesRemainingById.get(responderId);
    }

    public void waitForResponses() {
        int newTimeoutMs = this.currentTimeoutMs - toIntExact(clock.instant().toEpochMilli() - this.startedAt);
        LOG.debug("[correlation id: {}] Waiting for responses until {}.", requestMessage.getCorrelationId(), clock.instant().plus(newTimeoutMs, ChronoUnit.MILLIS));
        this.responseTimeoutFuture = timeoutManager.enableResponseTimeout(newTimeoutMs, this);
    }

    void waitForAcks() {
        if (ackTimeoutFuture == null) {
            LOG.debug("[correlation id: {}] Waiting for ack until {}.", requestMessage.getCorrelationId(), this.waitForAcksUntil);
            long ackTimeoutMs = waitForAcksUntil.toEpochMilli() - clock.instant().toEpochMilli();
            ackTimeoutFuture = timeoutManager.enableAckTimeout(toIntExact(ackTimeoutMs), this);
        } else {
            LOG.debug("[correlation id: {}] Ack timeout is already scheduled", requestMessage.getCorrelationId());
        }
    }

    private int getResponseTimeoutFromConfigs(RequestOptions requestOptions) {
        if (requestOptions.getResponseTimeout() == null) {
            return 3000;
        }
        return requestOptions.getResponseTimeout();
    }

    private void cancelResponseTimeoutTask() {
        if (this.responseTimeoutFuture != null) {
            responseTimeoutFuture.cancel(true);
        }
    }

    private void cancelAckTimeoutTask() {
        if (this.ackTimeoutFuture != null) {
            ackTimeoutFuture.cancel(true);
        }
    }

    List<Message> getAckMessages() {
        return ackMessages;
    }

    List<Message> getPayloadMessages() {
        return payloadMessages;
    }

    Message getRequestMessage() {
        return requestMessage;
    }

    @Override
    public boolean forceDirectInvocation(){
        return directlyInvokable;
    }
}
