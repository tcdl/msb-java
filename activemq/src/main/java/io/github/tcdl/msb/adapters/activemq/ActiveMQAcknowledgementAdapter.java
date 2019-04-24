package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.acknowledge.AcknowledgementAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;

/**
 * ActiveMQ acknowledgement implementation.
 */
public class ActiveMQAcknowledgementAdapter implements AcknowledgementAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQAcknowledgementAdapter.class);

    private Message message;

    public ActiveMQAcknowledgementAdapter(Message message) {
        this.message = message;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void confirm() throws Exception {
        message.acknowledge();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reject() throws Exception {
        message.acknowledge();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void retry() throws Exception {
        //Do nothing. Unconfirmed message will not be removed from the queue
    }
}

