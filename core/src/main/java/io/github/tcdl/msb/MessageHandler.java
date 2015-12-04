package io.github.tcdl.msb;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.message.Message;

public interface MessageHandler {

    /**
     * The handleMessage method is invoked when a message is successfully parsed and is ready for processing.
     *
     * @param message the message content
     * @param acknowledgeHandler confirm/reject message handler
     */
    void handleMessage(Message message, ConsumerAdapter.AcknowledgementHandler acknowledgeHandler);
}
