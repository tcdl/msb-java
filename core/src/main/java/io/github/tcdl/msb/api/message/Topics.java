package io.github.tcdl.msb.api.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.Validate;

public final class Topics {

    private final String to;
    private final String response;
    private final String forward;
    private final String routingKey;

    @JsonCreator
    public Topics(@JsonProperty("to") String to,
                  @JsonProperty("response") String response,
                  @JsonProperty("forward") String forward,
                  @JsonProperty("routingKey") String routingKey) {

        Validate.notNull(to, "the 'to' must not be null");
        this.to = to;
        this.response = response;
        this.forward = forward;
        this.routingKey = routingKey;
    }

    public Topics(String to, String response, String forward) {
        this(to, response, forward, null);
    }

    public String getTo() {
        return to;
    }

    public String getResponse() {
        return response;
    }

    public String getForward() {
        return forward;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    @Override
    public String toString() {
        return "Topics [to=" + to + ", response=" + response + ", forward=" + forward + ", routingKey=" + routingKey + "]";
    }
}
