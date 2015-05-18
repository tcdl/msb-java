package io.github.tcdl;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.EventHandler;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.exception.JsonSchemaValidationException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rdro on 4/23/2015.
 */
public class Consumer {
	
	public static Logger log = LoggerFactory.getLogger(Consumer.class);

	private Adapter rawAdapter;
	private String topic;
	private EventHandler messageHandler;
	private MsbConfigurations msbConfig; // not sure we need this here
	MsbMessageOptions msgOptions;

	public Consumer(String topic, MsbConfigurations msbConfig,
			MsbMessageOptions msgOptions) {

		this.topic = topic;
		this.msbConfig = msbConfig;
		this.msgOptions = msgOptions;

		// just stub,will be provided by init and provided
		this.rawAdapter = AdapterFactory.getInstance().createAdapter(msbConfig.getBrokerType(), topic);
	}

	public Consumer subscribe() {
		// merge msgOptions with msbConfig
		// do other stuff
		rawAdapter.subscribe(new Adapter.RawMessageHandler() {
					public void onMessage(String jsonMessage) {
						log.debug("Message recieved {}", jsonMessage);
						Exception error = null;
						Message message = null;

						try {
							if (msbConfig.getSchema() != null
									&& !isServiceChannel(topic)) {
								Utils.validateJsonWithSchema(jsonMessage,
										msbConfig.getSchema());
							}
							message = (Message) Utils.fromJson(jsonMessage,
									Message.class);
						} catch (JsonConversionException | JsonSchemaValidationException e) {
							error = e;
						}

						if (messageHandler != null) {
							messageHandler.onEvent(message, error);
						}
					}					
				});
		return this;
	}

    public Consumer withMessageHandler(EventHandler messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    public void end() {
    }

    private boolean isServiceChannel(String topic) {
        return topic.charAt(0) == '_';
    }
}
