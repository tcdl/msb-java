package io.github.tcdl.msb.api;

import org.apache.commons.lang3.Validate;

public class MessageDestination {

    private final String topic;
    private final String routingKey;

    public MessageDestination(String topic, String routingKey) {
        Validate.notNull(topic, "topic is mandatory");
        Validate.notNull(routingKey, "routingKey is mandatory");
        this.topic = topic;
        this.routingKey = routingKey;
    }

    public String getTopic() {
        return topic;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageDestination that = (MessageDestination) o;

        if (!topic.equals(that.topic)) return false;
        return routingKey.equals(that.routingKey);

    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + routingKey.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MessageDestination{" +
                "topic='" + topic + '\'' +
                ", routingKey='" + routingKey + '\'' +
                '}';
    }
}
