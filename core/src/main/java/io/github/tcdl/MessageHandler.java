package io.github.tcdl;

import io.github.tcdl.api.message.Message;

public interface MessageHandler {

    /**
     * Invoked when a message is successfully parsed and is ready for processing
     */
    void handleMessage(Message message);
}
