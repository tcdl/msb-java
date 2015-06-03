package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.EventHandlers;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static io.github.tcdl.support.Utils.ifNull;

/**
 * Created by rdro on 4/23/2015.
 */
public class Collector {

    public static final Logger LOG = LoggerFactory.getLogger(Collector.class);

    protected ChannelManager channelManager;
    private List<Message> ackMessages;
    private List<Message> payloadMessages;

    private Map<String, Integer> timeoutMsById;
    private Map<String, Integer> responsesRemainingById;

    private int timeoutMs;
    private int currentTimeoutMs;
    private long waitForAcksUntil;
    private int waitForResponses;
    private int responsesRemaining;

    private Long startedAt;
    private  final TimerProvider timer;

    private Clock clock;

    private String topic;

    private Optional<Callback<Payload>> onResponse = Optional.empty();
    private Optional<Callback<Acknowledge>> onAcknowledge = Optional.empty();
    private Optional<Callback<Exception>> onError = Optional.empty();
    private Optional<Callback<List<Message>>> onEnd = Optional.empty();

    public Collector(MsbMessageOptions messageOptions, MsbContext msbContext, EventHandlers eventHandlers) {
        this.channelManager = msbContext.getChannelManager();
        this.clock = msbContext.getClock();

        this.startedAt = clock.instant().toEpochMilli();

        this.ackMessages = new LinkedList<>();
        this.payloadMessages = new LinkedList<>();
        this.timeoutMsById = new HashMap<>();
        this.responsesRemainingById = new HashMap<>();

        this.timeoutMs = getResponseTimeoutFromConfigs(messageOptions);
        this.currentTimeoutMs = timeoutMs;

        this.waitForAcksUntil = getWaitForAckUntilFromConfigs(messageOptions);
        this.waitForResponses = getWaitForResponsesFromConfigs(messageOptions);
        this.responsesRemaining = waitForResponses;

        this.timer = new TimerProvider(this);

        if (eventHandlers != null) {
            onResponse = Optional.ofNullable(eventHandlers.onResponse());
            onAcknowledge = Optional.ofNullable(eventHandlers.onAcknowledge());
            onError = Optional.ofNullable(eventHandlers.onError());
            onEnd = Optional.ofNullable(eventHandlers.onEnd());
        }

    }

    public boolean isWaitForResponses() {
        return waitForResponses != 0;
    }

    public boolean isAwaitingAcks() {
        return waitForAcksUntil > clock.instant().toEpochMilli();
    }

    public boolean isAwaitingResponses() {
        return getResponsesRemaining() > 0;
    }

    public void listenForResponses(String topic, final Predicate<Message> shouldAcceptMessagePredicate) {
        Consumer consumer = channelManager.findOrCreateConsumer(topic);
        this.topic = topic;

        consumer.subscribe(
                message -> {
                    if (shouldAcceptMessagePredicate == null || shouldAcceptMessagePredicate.test(message)) {
                        handleMessage(message);
                    }
                },
                this::handleError
        );
    }

    protected void handleMessage(Message message) {
        LOG.debug("Received {}", message);

        if (message.getPayload() != null) {
            LOG.debug("Received {}", message.getPayload());
            payloadMessages.add(message);

            onResponse.ifPresent(handler -> handler.call(message.getPayload()));
            incResponsesRemaining(-1);
        } else {
            LOG.debug("Received {}", message.getAck());
            ackMessages.add(message);
            onAcknowledge.ifPresent(handler -> handler.call((message.getAck())));
        }

        processAck(message.getAck());

        if (isAwaitingResponses()) {
            waitForResponses();
            return;
        }

        if (isAwaitingAcks()) {
            waitForAcks();
            return;
        }

        end();
    }

    protected void handleError(Exception exception) {
        onError.ifPresent(handler -> handler.call(exception));
    }

    protected void end() {
        LOG.debug("Stop response processing");

        this.timer.stopTimers();
        channelManager.removeConsumer(topic);
        onEnd.ifPresent(handler -> handler.call(payloadMessages));
    }

    protected void waitForResponses() {
        LOG.debug("Waiting for responses until {}.", Instant.ofEpochMilli(System.currentTimeMillis() + this.currentTimeoutMs)); // FIXME
        timer.enableResponseTimeout(this.currentTimeoutMs);
    }

    private void waitForAcks() {
        LOG.debug("Waiting for ack until {}.", Instant.ofEpochMilli(this.waitForAcksUntil)); // FIXME
        long ackTimeoutMs = waitForAcksUntil - System.currentTimeMillis(); // FIXME
        timer.enableAckTimeout(ackTimeoutMs);
    }

    private void processAck(Acknowledge acknowledge) {
        if (acknowledge == null)
            return;

        if (acknowledge.getTimeoutMs() != null) {
            Integer newTimeoutMs = setTimeoutMsForResponderId(acknowledge.getResponderId(), acknowledge.getTimeoutMs());
            if (newTimeoutMs != null) {
                int prevTimeoutMs = this.currentTimeoutMs;

                this.currentTimeoutMs = getMaxTimeoutMs();
                if (prevTimeoutMs != currentTimeoutMs) {
                    timer.enableResponseTimeout(this.currentTimeoutMs);
                }
            }
        }

        if (acknowledge.getResponsesRemaining() != null) {
            LOG.debug("Remaining responses {}",
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

    private Integer getResponsesRemaining() {
        if (responsesRemainingById == null || responsesRemainingById.isEmpty()) {
            return responsesRemaining;
        }

        Integer sumOfResponsesRemaining = 0;
        for (Integer responses : responsesRemainingById.values()) {
            sumOfResponsesRemaining += responses;
        }

        return Math.max(responsesRemaining, sumOfResponsesRemaining);
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

    private int setResponsesRemainingForResponderId(String responderId, Integer responsesRemaining) {
        boolean notChanged = (responsesRemainingById != null && responsesRemainingById.containsKey(responderId) && responsesRemainingById
                .get(responderId).equals(responsesRemaining));
        if (notChanged)
            return 0;

        boolean atMin = (responsesRemaining < 0 && (responsesRemainingById.isEmpty() || !responsesRemainingById
                .containsKey(responderId)));
        if (atMin)
            return 0;

        if (responsesRemaining == 0) {
            responsesRemainingById.put(responderId, 0);
        } else {
            responsesRemainingById.put(responderId,
                    Math.max(0, ifNull(responsesRemainingById.get(responderId), 0) + responsesRemaining));
        }

        return responsesRemainingById.get(responderId);
    }

    private int getResponseTimeoutFromConfigs(MsbMessageOptions messageOptions) {
        if (messageOptions.getResponseTimeout() == null) {
            return 3000;
        }
        return messageOptions.getResponseTimeout();
    }

    private long getWaitForAckUntilFromConfigs(MsbMessageOptions messageOptions) {
        if (messageOptions.getAckTimeout() == null) {
            return 0;
        }
        return this.startedAt + messageOptions.getAckTimeout();
    }

    private int getWaitForResponsesFromConfigs(MsbMessageOptions messageOptions) {
        if (messageOptions.getWaitForResponses() == null || messageOptions.getWaitForResponses() == -1) {
            return 0;
        }
        return messageOptions.getWaitForResponses();
    }

    ChannelManager getChannelManager() {
        return channelManager;
    }

    List<Message> getAckMessages() {
        return ackMessages;
    }

    List<Message> getPayloadMessages() {
        return payloadMessages;
    }
}
