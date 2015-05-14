package tcdl.msb;

import java.util.HashMap;

import tcdl.msb.adapters.Adapter;
import tcdl.msb.adapters.AdapterFactory;
import tcdl.msb.config.MsbConfigurations;
import tcdl.msb.events.EventHandler;
import tcdl.msb.messages.Message;
import tcdl.msb.support.Utils;

/**
 * Created by rdro on 4/23/2015.
 */
public class Producer {

	private EventHandler messageHandler;
	private String topic;
	private Adapter rawAdapter;

	public Producer(String topic, MsbConfigurations msbConfig) {
		this.topic = topic;
		this.rawAdapter = AdapterFactory.getInstance().createAdapter(
				msbConfig.getBrokerType(), new HashMap<>());
	}

	public Producer publish(Message message) {
		String jsonMessage = Utils.toJson(message);
		Exception error = null;

		try {
			rawAdapter.publish(topic, jsonMessage);
		} catch (Exception e) {
			error = e;
		}

		if (messageHandler != null) {
            messageHandler.onEvent(message, error);
		}
		
		return this;
	}

    public Producer withMessageHandler(EventHandler messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }
}
