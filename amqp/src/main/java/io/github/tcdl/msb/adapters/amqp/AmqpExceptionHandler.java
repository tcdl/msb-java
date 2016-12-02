package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.DefaultExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpExceptionHandler extends DefaultExceptionHandler {

    private static Logger LOG = LoggerFactory.getLogger(AmqpExceptionHandler.class);

    @Override
    protected void handleChannelKiller(Channel channel, Throwable exception, String what) {
        LOG.error("{} threw exception for channel '{}'.", what, channel, exception);
        super.handleChannelKiller(channel, exception, what);
    }
}
