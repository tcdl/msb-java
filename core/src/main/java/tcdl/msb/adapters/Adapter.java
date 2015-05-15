package tcdl.msb.adapters;

import tcdl.msb.exception.ChannelException;

/**
 * Created by rdro on 4/24/2015.
 */
public interface Adapter {

    void publish(String jsonMessage) throws ChannelException;

    void subscribe(RawMessageHandler onMessageHandler);

    interface RawMessageHandler {
        void onMessage(String jsonMessage);
    }
}
