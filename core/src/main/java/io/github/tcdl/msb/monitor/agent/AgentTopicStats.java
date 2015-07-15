package io.github.tcdl.msb.monitor.agent;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable class that contains statistics for a topic.
 */
public class AgentTopicStats {
    /** Indicates whether this microservice produces to the topic */
    private boolean producers;

    /** Indicates whether this microservice consumes from the topic */
    private boolean consumers;

    /** Time when this microservice produced to the topic for the last time. */
    private Instant lastProducedAt;

    /** Time when this microservice consumed from the topic for the last time */
    private Instant lastConsumedAt;

    public AgentTopicStats() {
    }

    @SuppressWarnings("unused")
    public AgentTopicStats(@JsonProperty boolean producers, @JsonProperty boolean consumers, @JsonProperty Instant lastProducedAt, @JsonProperty Instant lastConsumedAt) {
        this.producers = producers;
        this.consumers = consumers;
        this.lastProducedAt = lastProducedAt;
        this.lastConsumedAt = lastConsumedAt;
    }

    public AgentTopicStats(AgentTopicStats agentTopicStats) {
        this.consumers = agentTopicStats.consumers;
        this.producers = agentTopicStats.producers;
        this.lastConsumedAt = agentTopicStats.lastConsumedAt;
        this.lastProducedAt = agentTopicStats.lastProducedAt;
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

    public AgentTopicStats withProducers(boolean producers) {
        AgentTopicStats newTopic = new AgentTopicStats(this);
        newTopic.producers = producers;
        return newTopic;
    }

    public AgentTopicStats withConsumers(boolean consumers) {
        AgentTopicStats newTopic = new AgentTopicStats(this);
        newTopic.consumers = consumers;
        return newTopic;
    }

    public AgentTopicStats withLastProducedAt(Instant lastProducedAt) {
        AgentTopicStats newTopic = new AgentTopicStats(this);
        newTopic.lastProducedAt = lastProducedAt;
        return newTopic;
    }

    public AgentTopicStats withLastConsumedAt(Instant lastConsumedAt) {
        AgentTopicStats newTopic = new AgentTopicStats(this);
        newTopic.lastConsumedAt = lastConsumedAt;
        return newTopic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AgentTopicStats that = (AgentTopicStats) o;
        return Objects.equals(producers, that.producers) &&
                Objects.equals(consumers, that.consumers) &&
                Objects.equals(lastProducedAt, that.lastProducedAt) &&
                Objects.equals(lastConsumedAt, that.lastConsumedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producers, consumers, lastProducedAt, lastConsumedAt);
    }

    @Override public String toString() {
        return String.format("AgentTopicStats [producers=%s, consumers=%s, lastProducedAt=%s, lastConsumedAt=%s]", producers, consumers, lastProducedAt,
                lastConsumedAt);
    }
}