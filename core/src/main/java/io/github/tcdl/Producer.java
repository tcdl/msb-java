package io.github.tcdl;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.events.EventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.Utils;

/**
 * Created by rdro on 4/23/2015.
 */
public class Producer {

	private EventHandler messageHandler;
	private String topic;
	private Adapter rawAdapter;

	public Producer(String topic, MsbConfigurations msbConfig) {
		this.topic = topic;
		this.rawAdapter = AdapterFactory.getInstance().createAdapter(msbConfig.getBrokerType(), topic);
	}

	public Producer publish(Message message) {
		String jsonMessage = Utils.toJson(message);
		Exception error = null;

		try {
			rawAdapter.publish(jsonMessage);
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
