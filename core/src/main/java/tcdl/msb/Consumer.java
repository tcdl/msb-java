package tcdl.msb;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tcdl.msb.adapters.Adapter;
import tcdl.msb.adapters.AdapterFactory;
import tcdl.msb.config.MsbConfigurations;
import tcdl.msb.config.MsbMessageOptions;
import tcdl.msb.events.EventHandler;
import tcdl.msb.exception.JsonConversionException;
import tcdl.msb.exception.JsonSchemaValidationException;
import tcdl.msb.messages.Message;
import tcdl.msb.support.Utils;

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
		this.rawAdapter = AdapterFactory.getInstance().createAdapter(
				msbConfig.getBrokerType(), new HashMap<String, String>());
	}

	public Consumer subscribe() {
		// merge msgOptions with msbConfig
		// do other stuff
		rawAdapter.subscribe(new HashMap<String, String>(),
				new Adapter.RawMessageHandler() {
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
