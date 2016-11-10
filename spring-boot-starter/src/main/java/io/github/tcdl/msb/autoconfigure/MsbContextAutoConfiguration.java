package io.github.tcdl.msb.autoconfigure;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.threading.MessageGroupStrategy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MsbContextAutoConfiguration {

    @Autowired
    MsbConfig msbConfig;

    @Autowired(required = false)
    MessageGroupStrategy messageGroupStrategy;

    @Bean
    @ConditionalOnMissingBean(MessageTemplate.class)
    public MessageTemplate messageTemplate() {
        return new MessageTemplate();
    }

    @Bean
    @ConditionalOnMissingBean(MsbContext.class)
    public MsbContext msbContext() {

        MsbContextBuilder builder = new MsbContextBuilder()
                .withMsbConfig(msbConfig);

        if (messageGroupStrategy != null)
            builder = builder.withMessageGroupStrategy(messageGroupStrategy);

        return builder.build();
    }
}
