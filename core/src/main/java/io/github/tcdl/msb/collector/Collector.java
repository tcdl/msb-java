package io.github.tcdl.msb.collector;

import static io.github.tcdl.msb.support.Utils.ifNull;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.events.EventHandlers;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.support.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Collector} is a component which collects responses and acknowledgements for sent requests.
 */
public class Collector<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Collector.class);

    private static final int WAIT_FOR_RESPONSES_UNTIL_TIMEOUT = -1;

    private List<Message> ackMessages;
    private List<Message> payloadMessages;

    private Map<String, Integer> timeoutMsById;
    private Map<String, Integer> responsesRemainingById;

    private int timeoutMs;
    private int currentTimeoutMs;
    private Integer waitForAcksMs;
    private Long waitForAcksUntil;
    private int waitForResponses;
    private TypeReference<T> payloadTypeReference;
    private int responsesRemaining;

    private Long startedAt;
    private TimeoutManager timeoutManager;
    private ObjectMapper payloadMapper;

    private Clock clock;
    private Message requestMessage;

    private Optional<Callback<Message>> onRawResponse = Optional.empty();
    private Optional<Callback<T>> onResponse = Optional.empty();
    private Optional<Callback<Acknowledge>> onAcknowledge = Optional.empty();
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
        this.waitForResponses = getWaitForResponsesFromConfigs(requestOptions);

        this.timeoutMs = getResponseTimeoutFromConfigs(requestOptions);
        this.currentTimeoutMs = timeoutMs;

        this.responsesRemaining = waitForResponses;
        this.payloadTypeReference = payloadTypeReference;

        if (eventHandlers != null) {
            onRawResponse = Optional.ofNullable(eventHandlers.onRawResponse());
            onResponse = Optional.ofNullable(eventHandlers.onResponse());
            onAcknowledge = Optional.ofNullable(eventHandlers.onAcknowledge());
            onEnd = Optional.ofNullable(eventHandlers.onEnd());
        }
    }

    boolean isAwaitingAcks() {
        return this.waitForAcksUntil != null && waitForAcksUntil > clock.instant().toEpochMilli();
    }

    boolean isAwaitingResponses() {
        return getResponsesRemaining() > 0;
    }

    public void listenForResponses() {
        if (this.waitForAcksMs != null && this.waitForAcksMs != 0) {
            this.waitForAcksUntil = this.startedAt + waitForAcksMs;
        }
        collectorManager.registerCollector(this);
    }

    public void handleMessage(Message incomingMessage) {
        LOG.debug("Received {}", incomingMessage);

        JsonNode rawPayload = incomingMessage.getRawPayload();
        if (Utils.isPayloadPresent(rawPayload)) {
            LOG.debug("Received Payload {}", rawPayload);
            payloadMessages.add(incomingMessage);
            onRawResponse.ifPresent(handler -> handler.call(incomingMessage));

            T payload = Utils.convert(rawPayload, payloadTypeReference, payloadMapper);
            onResponse.ifPresent(handler -> handler.call(payload));

            incResponsesRemaining(-1);
        } else {
            LOG.debug("Received {}", incomingMessage.getAck());
            ackMessages.add(incomingMessage);
            onAcknowledge.ifPresent(handler -> handler.call((incomingMessage.getAck())));
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

    private void processAck(Acknowledge acknowledge) {
        if (acknowledge == null)
            return;

        if (acknowledge.getTimeoutMs() != null && acknowledge.getResponderId() != null) {
            Integer newTimeoutMs = setTimeoutMsForResponderId(acknowledge.getResponderId(), acknowledge.getTimeoutMs());
            if (newTimeoutMs != null) {
                int prevTimeoutMs = this.currentTimeoutMs;

                this.currentTimeoutMs = getMaxTimeoutMs();
                if (prevTimeoutMs != currentTimeoutMs) {
                    cancelResponseTimeoutTask();
                    this.responseTimeoutFuture = timeoutManager.enableResponseTimeout(this.currentTimeoutMs, this);
                }
            }
        }

        if (acknowledge.getResponsesRemaining() != null) {
            LOG.debug("Responses remaining for responderId [{}] is set to {}", acknowledge.getResponderId(),
                    setResponsesRemainingForResponderId(acknowledge.getResponderId(), acknowledge.getResponsesRemaining()));
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

    private int getWaitForResponsesFromConfigs(RequestOptions requestOptions) {
        if (requestOptions.getWaitForResponses() == null || requestOptions.getWaitForResponses() == WAIT_FOR_RESPONSES_UNTIL_TIMEOUT) {
            // use for Infinity number or expected responses
            return Integer.MAX_VALUE;
        } else {
            return requestOptions.getWaitForResponses();
        }
    }

    public void waitForResponses() {
        LOG.debug("Waiting for responses until {}.", clock.instant().plus(currentTimeoutMs, ChronoUnit.MILLIS));
        this.responseTimeoutFuture = timeoutManager.enableResponseTimeout(this.currentTimeoutMs, this);
    }

    private void waitForAcks() {
        if (ackTimeoutFuture == null) {
            LOG.debug("Waiting for ack until {}.", Instant.ofEpochMilli(this.waitForAcksUntil));
            long ackTimeoutMs = waitForAcksUntil - clock.instant().toEpochMilli();
            ackTimeoutFuture = timeoutManager.enableAckTimeout(ackTimeoutMs, this);
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
