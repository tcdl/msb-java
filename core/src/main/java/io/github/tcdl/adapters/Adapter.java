package io.github.tcdl.adapters;

import io.github.tcdl.exception.ChannelException;

/**
 * Created by rdro on 4/24/2015.
 */
public interface Adapter {

    void publish(String jsonMessage) throws ChannelException;

    void subscribe(RawMessageHandler onMessageHandler);

    void unsubscribe();

    interface RawMessageHandler {
        void onMessage(String jsonMessage);
    }
}
