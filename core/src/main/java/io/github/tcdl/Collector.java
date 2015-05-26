package io.github.tcdl;

import static io.github.tcdl.events.Event.ACKNOWLEDGE_EVENT;
import static io.github.tcdl.events.Event.END_EVENT;
import static io.github.tcdl.events.Event.MESSAGE_EVENT;
import static io.github.tcdl.events.Event.PAYLOAD_EVENT;
import static io.github.tcdl.events.Event.RESPONSE_EVENT;
import static io.github.tcdl.support.Utils.ifNull;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Predicate;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rdro on 4/23/2015.
 */
public class Collector {

    public static final Logger LOG = LoggerFactory.getLogger(Collector.class);

    protected ChannelManager channelManager = ChannelManager.getInstance();
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
    private TimerTask timeout;
    private TimerTask ackTimeout;

    private String topic;

    public Collector(MsbMessageOptions config) {  
        Validate.notNull(config, "the 'config' must not be null");
        this.startedAt = System.currentTimeMillis();
        this.ackMessages = new LinkedList<>();
        this.payloadMessages = new LinkedList<>();
        this.timeoutMsById = new HashMap<>();
        this.responsesRemainingById = new HashMap<>();

        this.timeoutMs = getResponseTimeout(config);
        this.currentTimeoutMs = timeoutMs;

        this.waitForAcksUntil = config.getAckTimeout() != null
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
            LOG.debug("Received {}", message);

            if (message.getPayload() != null) {
                LOG.debug("Received {}", message.getPayload());
                payloadMessages.add(message);
                channelManager.emit(PAYLOAD_EVENT, message.getPayload());
                channelManager.emit(RESPONSE_EVENT, message.getPayload());
                incResponsesRemaining(-1);
            } else {
                LOG.debug("Received {}", message.getAck());
                ackMessages.add(message);
                channelManager.emit(ACKNOWLEDGE_EVENT, message.getAck());
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
        this.topic = topic;
    }

    protected void cancel() {
        clearTimeout(timeout);
        clearTimeout(ackTimeout);
        channelManager.removeConsumer(topic);
    }

    protected void end() {
        LOG.debug("End");
        cancel();
        channelManager.emit(END_EVENT);
    }

    protected void enableTimeout() {
        clearTimeout(timeout);

        timeout = new TimerTask() {
            @Override
            public void run() {
                LOG.debug("Running enableTimeout() TimerTask");
                end();
            }
        };

        LOG.debug("Enabling response timeout for {} ms", currentTimeoutMs);
        setTimeout(timeout, currentTimeoutMs);
    }

    protected void enableAckTimeout() {
        if (ackTimeout != null)
            return;

        ackTimeout = new TimerTask() {
            @Override
            public void run() {
                if (isAwaitingResponses()) {
                    LOG.debug("Ack timer task run, but waiting for responses. No END event fired");
                    return;
                }
                LOG.debug("Running enableAckTimeout() TimerTask");
                end();
            }
        };

        long ackTimeoutMs = waitForAcksUntil - System.currentTimeMillis();
        LOG.debug("Waiting for ack until {}. Enabling ack timeout for {} ms", new Date(waitForAcksUntil), ackTimeoutMs);
        setTimeout(ackTimeout, ackTimeoutMs);
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
                    enableTimeout();
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

    private void setTimeout(TimerTask onTimeout, long delay) {
        new Timer().schedule(onTimeout, delay);
    }

    private void clearTimeout(TimerTask onTimeout) {
        if (onTimeout != null) {
            onTimeout.cancel();
        }
    }

    private int getResponseTimeout(MsbMessageOptions messageConfigs) {
        if (messageConfigs.getResponseTimeout() == null) {
            return 3000;
        }
        return messageConfigs.getResponseTimeout();
    }

    private int getWaitForResponses(MsbMessageOptions messageConfigs) {
        if (messageConfigs.getWaitForResponses() == null || messageConfigs.getWaitForResponses() == -1) {
            return 0;
        }
        return messageConfigs.getWaitForResponses();
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
