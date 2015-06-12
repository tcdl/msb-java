package io.github.tcdl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.support.JsonValidator;

import java.time.Clock;

/**
 * Contains all singletone beans required for MSB
 *
 * Created by rdro on 5/27/2015.
 */
public class MsbContext {

    private MsbConfigurations msbConfig;
    private MessageFactory messageFactory;
    private ChannelManager channelManager;
    private Clock clock;

    public MsbContext(MsbConfigurations msbConfig, MessageFactory messageFactory, ChannelManager channelManager, Clock clock) {
        this.msbConfig = msbConfig;
        this.messageFactory = messageFactory;
        this.channelManager = channelManager;
        this.clock = clock;
    }

    public MsbConfigurations getMsbConfig() {
        return msbConfig;
    }

    public void setMsbConfig(MsbConfigurations msbConfig) {
        this.msbConfig = msbConfig;
    }

    public MessageFactory getMessageFactory() {
        return messageFactory;
    }

    public void setMessageFactory(MessageFactory messageFactory) {
        this.messageFactory = messageFactory;
    }

    public ChannelManager getChannelManager() {
        return channelManager;
    }

    public void setChannelManager(ChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    public Clock getClock() {
        return clock;
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    public static class MsbContextBuilder {
        public MsbContext build() {
            Clock clock = Clock.systemDefaultZone();
            Config config = ConfigFactory.load();
            JsonValidator validator = new JsonValidator();
            MsbConfigurations msbConfig = new MsbConfigurations(config);
            ChannelManager channelManager = new ChannelManager(msbConfig, clock, validator);
            MessageFactory messageFactory = new MessageFactory(msbConfig.getServiceDetails(), clock);

            return new MsbContext(msbConfig, messageFactory, channelManager, clock);
        }
    }
}
