package io.github.tcdl.msb.api.monitor;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Represents statistics for the given topic
 */
public class AggregatorTopicStats {
    /**
     * Instance ids of services producing to this topic
     */
    private Set<String> producers = new ConcurrentSkipListSet<>();

    /**
     * Instance ids of services consuming from this topics
     */
    private Set<String> consumers = new ConcurrentSkipListSet<>();

    /**
     * Time when the last message was produced to the topic
     */
    private Instant lastProducedAt;

    /**
     * Time when the last message was consumed from the topic
     */
    private Instant lastConsumedAt;

    public AggregatorTopicStats() {
    }

    public AggregatorTopicStats(@Nullable AggregatorTopicStats aggregatorTopicStats) {
        if (aggregatorTopicStats != null) {
            this.producers = aggregatorTopicStats.producers;
            this.consumers = aggregatorTopicStats.consumers;
            this.lastProducedAt = aggregatorTopicStats.lastProducedAt;
            this.lastConsumedAt = aggregatorTopicStats.lastConsumedAt;
        }
    }

    public AggregatorTopicStats withProducers(Set<String> producers) {
        this.producers = producers;
        return this;
    }

    public AggregatorTopicStats withConsumers(Set<String> consumers) {
        this.consumers = consumers;
        return this;
    }

    public AggregatorTopicStats withLastProducedAt(Instant lastProducedAt) {
        this.lastProducedAt = lastProducedAt;
        return this;
    }

    public AggregatorTopicStats withLastConsumedAt(Instant lastConsumedAt) {
        this.lastConsumedAt = lastConsumedAt;
        return this;
    }

    public Set<String> getProducers() {
        return producers;
    }

    public Set<String> getConsumers() {
        return consumers;
    }

    public Instant getLastProducedAt() {
        return lastProducedAt;
    }

    public Instant getLastConsumedAt() {
        return lastConsumedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AggregatorTopicStats that = (AggregatorTopicStats) o;
        return Objects.equals(producers, that.producers) &&
                Objects.equals(consumers, that.consumers) &&
                Objects.equals(lastProducedAt, that.lastProducedAt) &&
                Objects.equals(lastConsumedAt, that.lastConsumedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producers, consumers, lastProducedAt, lastConsumedAt);
    }

    @Override
    public String toString() {
        return String.format("AggregatorTopicStats [producers=%s, consumers=%s, lastProducedAt=%s, lastConsumedAt=%s]", producers, consumers, lastProducedAt,
                lastConsumedAt);
    }
}
