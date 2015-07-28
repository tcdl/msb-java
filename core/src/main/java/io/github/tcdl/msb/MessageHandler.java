package io.github.tcdl.msb;

import io.github.tcdl.msb.api.message.Message;

public interface MessageHandler {

    /**
     * The handleMessage method is invoked when a message is successfully parsed and is ready for processing.
     *
     * @param message the message content
     */
    void handleMessage(Message message);
}
