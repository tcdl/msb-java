package io.github.tcdl.msb.collector;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.api.AcknowledgementHandler;
import io.github.tcdl.msb.api.message.Message;

/**
 * Interface used to notify {@link io.github.tcdl.msb.MessageHandler} regarding a count
 * of expected  {@link io.github.tcdl.msb.MessageHandler#handleMessage(Message, AcknowledgementHandler)} invocations.
 */
public interface ConsumedMessagesAwareMessageHandler extends MessageHandler {
    /**
     * Should be invoked when an incoming message has been consumed so {@link io.github.tcdl.msb.MessageHandler#handleMessage(Message, AcknowledgementHandler)}
     * will be invoked to process it in future.
     */
    void notifyMessageConsumed();

    /**
     * Should be invoked when an incoming message that was consumed previously has been lost so {@link io.github.tcdl.msb.MessageHandler#handleMessage(Message, AcknowledgementHandler)}
     * invocation is no longer expected.
     */
    void notifyConsumedMessageIsLost();
}
