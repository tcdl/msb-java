package io.github.tcdl;

import static io.github.tcdl.support.Utils.ifNull;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Predicate;

import io.github.tcdl.config.MsbMessageOptions;
import static io.github.tcdl.events.Event.*;
import io.github.tcdl.events.EventEmitter;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;

/**
 * Created by rdro on 4/23/2015.
 */
public class Collector extends EventEmitter {

    protected ChannelManager channelManager;
    private List<Message> ackMessages;
    private List<Message> payloadMessages;
    private List<Message> responseMessages;

    private Map<String, Integer> timeoutMsById;
    private Map<String, Integer> responsesRemainingById;

    private int timeoutMs;
    private int currentTimeoutMs;
    private long waitForAcksUntil;
    private int waitForResponses;
    private int responsesRemaining;

    private Long startedAt;
    private Timer timeout;
    private Timer ackTimeout;

    public Collector(MsbMessageOptions config) {
        this.channelManager = ChannelManager.getInstance();
        this.startedAt = System.currentTimeMillis();

        this.ackMessages = new LinkedList<>();
        this.payloadMessages = new LinkedList<>();
        this.responseMessages = this.payloadMessages;
        this.timeoutMsById = new HashMap<>();
        this.responsesRemainingById = new HashMap<>();

        this.timeoutMs = getResponseTimeout(config);
        this.currentTimeoutMs = timeoutMs;

        this.waitForAcksUntil = config != null && config.getAckTimeout() != null
                ? startedAt + config.getAckTimeout() : 0;

        this.waitForResponses = getWaitForResponses(config);
        this.responsesRemaining = waitForResponses;
    }

    public boolean isWaitForResponses() {
        return waitForResponses != 0;
    }

    public boolean isAwaitingAcks() {
        return waitForAcksUntil > System.currentTimeMillis();
    }

    public boolean isAwaitingResponses() {
        return getResponsesRemaining() > 0;
    }

    public void listenForResponses(String topic, final Predicate<Message> shouldAcceptMessagePredicate) {
        channelManager.on(MESSAGE_EVENT, (Message message) -> {
                if (shouldAcceptMessagePredicate != null && !shouldAcceptMessagePredicate.test(message)) {
                    return;
                }

                if (message.getPayload() != null) {
                    payloadMessages.add(message);
                    emit(PAYLOAD_EVENT, message.getPayload());
                    emit(RESPONSE_EVENT, message.getPayload());
                    incResponsesRemaining(-1);
                } else {
                    ackMessages.add(message);
                    emit(ACKNOWLEDGE_EVENT, message.getAck());
                }

                processAck(message.getAck());

                if (isAwaitingResponses())
                    return;
                if (isAwaitingAcks()) {
                    enableAckTimeout();
                    return;
                }

                end();
        });
        channelManager.findOrCreateConsumer(topic, null);
    }

    public void removeListeners() {
        channelManager.removeListeners(MESSAGE_EVENT);
    }

    protected void cancel() {
        clearTimeout(timeout);
        clearTimeout(ackTimeout);
        removeListeners();
    }

    protected void end() {
        cancel();
        emit(END_EVENT);
    }

    protected void enableTimeout() {
        clearTimeout(timeout);
        Long newTimeoutMs = currentTimeoutMs - (System.currentTimeMillis() - startedAt);
        timeout = setTimeout(onTimeout, newTimeoutMs);
    }

    private TimerTask onTimeout = new TimerTask() {
        @Override
        public void run() {
            end();
        }
    };

    protected void enableAckTimeout() {
        if (ackTimeout != null)
            return;
        ackTimeout = setTimeout(onAckTimeout, waitForAcksUntil - System.currentTimeMillis());
    }

    private TimerTask onAckTimeout = new TimerTask() {
        @Override
        public void run() {
            if (isAwaitingResponses())
                return;
            end();
        }
    };

    private void processAck(Acknowledge acknowledge) {
        if (acknowledge == null)
            return;

        if (acknowledge.getTimeoutMs() != null) {
            Integer newTimeoutMs = setTimeoutMsForResponderId(acknowledge.getResponderId(), acknowledge.getTimeoutMs());
            if (newTimeoutMs != null) {
                int prevTimeoutMs = this.currentTimeoutMs;

                this.currentTimeoutMs = getMaxTimeoutMs();
                if (prevTimeoutMs != currentTimeoutMs) {
                    enableTimeout();
                }
            }
        }

        if (acknowledge.getResponsesRemaining() != null) {
            setResponsesRemainingForResponderId(acknowledge.getResponderId(), acknowledge.getResponsesRemaining());
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

    private Timer setTimeout(TimerTask onTimeout, long delay) {
        Timer timer = new Timer();
        timer.schedule(onTimeout, delay);
        return timer;
    }

    private void clearTimeout(Timer timer) {
        if (timer != null)
            timer.cancel();
    }

    private int getResponseTimeout(MsbMessageOptions messageConfigs) {
        if (messageConfigs == null || messageConfigs.getResponseTimeout() == null) {
            return 3000;
        }
        return messageConfigs.getResponseTimeout();
    }

    private int getWaitForResponses(MsbMessageOptions messageConfigs) {
        if (messageConfigs == null || messageConfigs.getWaitForResponses() == null
                || messageConfigs.getWaitForResponses() == null || messageConfigs.getWaitForResponses() == -1) {
            return 0;
        }
        return messageConfigs.getWaitForResponses();
    }
}
