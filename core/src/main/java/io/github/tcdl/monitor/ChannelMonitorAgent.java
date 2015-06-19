package io.github.tcdl.monitor;

/**
 * Observer interface that allows to subscribe to different events related to
 * consuming from and producing to topics.
 *
 * The implementation is intended to be injected into {@link io.github.tcdl.ChannelManager} instance.
 */
public interface ChannelMonitorAgent {
    /**
     * Fired when topic producer is created. Typically this happens just before publishing of the first message
     * to the given topic.
     *
     * @param topicName
     */
    void producerTopicCreated(String topicName);

    /**
     * Fired when consumer is created that listens to the messages on the given topic.
     *
     * @param topicName
     */
    void consumerTopicCreated(String topicName);

    /**
     * Fired when consumer is removed for the given topic. The consumer is not longer able to listen to any messages
     * on that topic.
     *
     * @param topicName
     */
    void consumerTopicRemoved(String topicName);

    /**
     * Fired when a message is sent to the given topic.
     *
     * @param topicName
     */
    void producerMessageSent(String topicName);

    /**
     * Fired when a message is consumed from the given topic.
     *
     * @param topicName
     */
    void consumerMessageReceived(String topicName);
}
