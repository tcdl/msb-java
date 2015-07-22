package io.github.tcdl.msb.monitor.agent;

/**
 * Empty implementation that does nothing for each event.
 */
public class NoopChannelMonitorAgent implements ChannelMonitorAgent {
    /** {@inheritDoc} */
    @Override
    public void producerTopicCreated(String topicName) {
    }

    /** {@inheritDoc} */
    @Override
    public void consumerTopicCreated(String topicName) {
    }

    /** {@inheritDoc} */
    @Override
    public void consumerTopicRemoved(String topicName) {
    }

    /** {@inheritDoc} */
    @Override
    public void producerMessageSent(String topicName) {
    }

    /** {@inheritDoc} */
    @Override
    public void consumerMessageReceived(String topicName) {
    }
}
