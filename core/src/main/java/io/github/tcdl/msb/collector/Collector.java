package io.github.tcdl.msb.collector;

import static io.github.tcdl.msb.support.Utils.ifNull;
import static java.lang.Math.toIntExact;
import io.github.tcdl.msb.api.AcknowledgementHandler;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.events.EventHandlers;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.support.Utils;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * {@link Collector} is a component which collects responses and acknowledgements for sent requests.
 */
public class Collector<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Collector.class);

    private List<Message> ackMessages;
    private List<Message> payloadMessages;

    private Map<String, Integer> timeoutMsById;
    private Map<String, Integer> responsesRemainingById;

    private int timeoutMs;
    private int currentTimeoutMs;
    private Integer waitForAcksMs;
    private Instant waitForAcksUntil;

    private int responsesRemaining;
    private boolean shouldWaitUntilResponseTimeout;

    private TypeReference<T> payloadTypeReference;

    private long startedAt;
    private TimeoutManager timeoutManager;
    private ObjectMapper payloadMapper;

    private Clock clock;
    private Message requestMessage;

    private Optional<BiConsumer<Message, AcknowledgementHandler>> onRawResponse = Optional.empty();
    private Optional<BiConsumer<T, AcknowledgementHandler>> onResponse = Optional.empty();
    private Optional<BiConsumer<Acknowledge, AcknowledgementHandler>> onAcknowledge = Optional.empty();
    private Optional<Callback<Void>> onEnd = Optional.empty();

    private ScheduledFuture ackTimeoutFuture;
    private ScheduledFuture responseTimeoutFuture;
    private CollectorManager collectorManager;

    public Collector(String topic, Message requestMessage, RequestOptions requestOptions, MsbContextImpl msbContext, EventHandlers<T> eventHandlers,
            TypeReference<T> payloadTypeReference) {
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

        this.waitForAcksMs = requestOptions.getAckTimeout();
        this.waitForAcksUntil = null;

        this.timeoutMs = getResponseTimeoutFromConfigs(requestOptions);
        this.currentTimeoutMs = timeoutMs;

        int waitForResponses = requestOptions.getWaitForResponses();

        this.responsesRemaining = waitForResponses;

        if (waitForResponses == RequestOptions.WAIT_FOR_RESPONSES_UNTIL_TIMEOUT) {
            shouldWaitUntilResponseTimeout = true;
        }

        this.payloadTypeReference = payloadTypeReference;

        if (eventHandlers != null) {
            onRawResponse = Optional.ofNullable(eventHandlers.onRawResponse());
            onResponse = Optional.ofNullable(eventHandlers.onResponse());
            onAcknowledge = Optional.ofNullable(eventHandlers.onAcknowledge());
            onEnd = Optional.ofNullable(eventHandlers.onEnd());
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

    public void handleMessage(Message incomingMessage, AcknowledgementHandler acknowledgeHandler) {
        LOG.debug("Received {}", incomingMessage);

        JsonNode rawPayload = incomingMessage.getRawPayload();
        if (Utils.isPayloadPresent(rawPayload)) {
            LOG.debug("Received Payload {}", rawPayload);
            payloadMessages.add(incomingMessage);
            onRawResponse.ifPresent(handler -> handler.accept(incomingMessage, acknowledgeHandler));

            T payload = Utils.convert(rawPayload, payloadTypeReference, payloadMapper);
            onResponse.ifPresent(handler -> handler.accept(payload, acknowledgeHandler));

            incResponsesRemaining(-1);
        } else {
            LOG.debug("Received {}", incomingMessage.getAck());
            ackMessages.add(incomingMessage);
            onAcknowledge.ifPresent(handler -> handler.accept(incomingMessage.getAck(), acknowledgeHandler));
        }

        processAck(incomingMessage.getAck());

        if (isAwaitingResponses())
            return;

        //set ack timer task in case we received ALL expected responses but still have to wait for ack
        if (isAwaitingAcks()) {
            waitForAcks();
            return;
        }

        end();
    }

    protected void end() {
        LOG.debug("Stop response processing");

        cancelAckTimeoutTask();
        cancelResponseTimeoutTask();

        collectorManager.unregisterCollector(this);
        onEnd.ifPresent(handler -> handler.call(null));
    }

    void processAck(Acknowledge acknowledge) {
        if (acknowledge == null)
            return;

        if (acknowledge.getResponsesRemaining() != null) {
            LOG.debug("Responses remaining for responderId [{}] is set to {}", acknowledge.getResponderId(),
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

    private Integer incResponsesRemaining(Integer inc) {
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
        LOG.debug("Waiting for responses until {}.", clock.instant().plus(newTimeoutMs, ChronoUnit.MILLIS));
        this.responseTimeoutFuture = timeoutManager.enableResponseTimeout(newTimeoutMs, this);
    }

    void waitForAcks() {
        if (ackTimeoutFuture == null) {
            LOG.debug("Waiting for ack until {}.", this.waitForAcksUntil);
            long ackTimeoutMs = waitForAcksUntil.toEpochMilli() - clock.instant().toEpochMilli();
            ackTimeoutFuture = timeoutManager.enableAckTimeout(toIntExact(ackTimeoutMs), this);
        } else {
            LOG.debug("Ack timeout is already scheduled");
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
}
