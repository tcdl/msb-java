package tcdl.msb.adapters;

import java.util.Map;

import tcdl.msb.exception.ChannelException;

/**
 * Created by rdro on 4/24/2015.
 */
public interface Adapter {

    void publish(String topic, String jsonMessage) throws ChannelException;

    void subscribe(Map<String,String> subscriberConfig, RawMessageHandler onMessageHandler);

    interface RawMessageHandler {
        void onMessage(String jsonMessage);
    }
}
