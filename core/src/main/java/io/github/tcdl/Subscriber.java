package io.github.tcdl;

import io.github.tcdl.messages.Message;

/**
 * Created by ruslan on 22.06.15.
 */
public interface Subscriber {

    /**
     * Invoked when a message is successfully parsed and is ready for processing
     */
    void handleMessage(Message message);
}
