package io.github.tcdl.msb.acknowledge;

import java.util.concurrent.atomic.AtomicBoolean;

import io.github.tcdl.msb.api.AcknowledgementHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides acknowledgement mechanism implementation that handles
 * both implicit (provided by {@link AcknowledgementHandlerInternal} and used by the library)
 * and explicit (provided by {@link AcknowledgementHandler} and used by library clients) messages acknowledge means.
 */
public class AcknowledgementHandlerImpl implements AcknowledgementHandlerInternal {

    private static final String ACK_WAS_ALREADY_SENT = "[%s] Acknowledgement was already sent during message processing.";

    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgementHandlerImpl.class);

    final AcknowledgementAdapter acknowledgementAdapter;
    final boolean isMessageRedelivered;
    final String messageTextIdentifier;

    final AtomicBoolean acknowledgementSent = new AtomicBoolean(false);
    volatile boolean autoAcknowledgement = true;

    public AcknowledgementHandlerImpl(AcknowledgementAdapter acknowledgementAdapter,
                                      boolean isMessageRedelivered, String messageTextIdentifier) {
        super();
        this.acknowledgementAdapter = acknowledgementAdapter;
        this.isMessageRedelivered = isMessageRedelivered;
        this.messageTextIdentifier = messageTextIdentifier;
    }

    public boolean isAutoAcknowledgement() {
        return autoAcknowledgement;
    }

    public void setAutoAcknowledgement(boolean autoAcknowledgement) {
        this.autoAcknowledgement = autoAcknowledgement;
    }

    @Override
    public void confirmMessage() {
        executeAck("confirm", () -> {
            acknowledgementAdapter.confirm();
            LOG.debug(String.format("[%s] A message was confirmed", messageTextIdentifier));
        });
    }

    @Override
    public void retryMessage() {
        executeAck("requeue", () -> {
            if(!isMessageRedelivered) {
                acknowledgementAdapter.retry();
                LOG.debug(String.format("[%s] A message was rejected with requeue", messageTextIdentifier));
            } else {
                acknowledgementAdapter.reject();
                LOG.warn(String.format("[%s] Can't requeue message because it already was redelivered once, discarding it instead", messageTextIdentifier));
            }
        });
    }

    @Override
    public void rejectMessage() {
        executeAck("reject", () -> {
            acknowledgementAdapter.reject();
            LOG.debug(String.format("[%s] A message was discarded", messageTextIdentifier));
        });
    }

    private void executeAck(String actionName, AckAction ackAction) {
        if (acknowledgementSent.compareAndSet(false, true)) {
            try {
                ackAction.perform();
            } catch (Exception e) {
                LOG.error(String.format("[%s] Got exception when trying to %s a message:", messageTextIdentifier, actionName), e);
            }
        } else {
            LOG.error(String.format(ACK_WAS_ALREADY_SENT, messageTextIdentifier));
        }
    }
    
    public void autoConfirm() {
        executeAutoAck(() -> {
            confirmMessage();
            LOG.debug(String.format("[%s] A message was automatically confirmed after message processing", messageTextIdentifier));
        });
    }

    public void autoReject() {
        executeAutoAck(() -> {
            rejectMessage();
            LOG.debug(String.format("[%s] A message was automatically rejected due to error during message processing", messageTextIdentifier));
        });
    }

    public void autoRetry() {
        executeAutoAck(() -> {
            retryMessage();
            LOG.debug(String.format("[%s] A message was automatically rejected (with a requeue attempt) due to error during message processing", messageTextIdentifier));
        });
    }

    private void executeAutoAck(AutoAckAction ackAction) {
        if (autoAcknowledgement && !acknowledgementSent.get()) {
            ackAction.perform();
        }
    }

    @FunctionalInterface
    private interface AckAction {
        void perform() throws Exception;
    }

    @FunctionalInterface
    private interface AutoAckAction {
        void perform();
    }

}
