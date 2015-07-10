package io.github.tcdl.msb.monitor;

import java.time.Instant;

/**
 * Immutable class that contains statistics for a topic.
 */
class TopicStats {
    /** Indicates whether this microservice produces to the topic */
    private boolean producers;

    /** Indicates whether this microservice consumes from the topic */
    private boolean consumers;

    /** Time when this microservice produced to the topic for the last time. */
    private Instant lastProducedAt;

    /** Time when this microservice consumed from the topic for the last time */
    private Instant lastConsumedAt;

    public TopicStats() {
    }

    public TopicStats(TopicStats topicStats) {
        this.consumers = topicStats.consumers;
        this.producers = topicStats.producers;
        this.lastConsumedAt = topicStats.lastConsumedAt;
        this.lastProducedAt = topicStats.lastProducedAt;
    }

    public boolean isProducers() {
        return producers;
    }

    public boolean isConsumers() {
        return consumers;
    }

    public Instant getLastProducedAt() {
        return lastProducedAt;
    }

    public Instant getLastConsumedAt() {
        return lastConsumedAt;
    }

    public TopicStats setProducers(boolean producers) {
        TopicStats newTopic = new TopicStats(this);
        newTopic.producers = producers;
        return newTopic;
    }

    public TopicStats setConsumers(boolean consumers) {
        TopicStats newTopic = new TopicStats(this);
        newTopic.consumers = consumers;
        return newTopic;
    }

    public TopicStats setLastProducedAt(Instant lastProducedAt) {
        TopicStats newTopic = new TopicStats(this);
        newTopic.lastProducedAt = lastProducedAt;
        return newTopic;
    }

    public TopicStats setLastConsumedAt(Instant lastConsumedAt) {
        TopicStats newTopic = new TopicStats(this);
        newTopic.lastConsumedAt = lastConsumedAt;
        return newTopic;
    }
}