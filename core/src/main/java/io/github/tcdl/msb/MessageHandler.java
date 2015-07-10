package io.github.tcdl.msb;

import io.github.tcdl.msb.api.message.Message;

public interface MessageHandler {

    /**
     * Invoked when a message is successfully parsed and is ready for processing
     */
    void handleMessage(Message message);
}
