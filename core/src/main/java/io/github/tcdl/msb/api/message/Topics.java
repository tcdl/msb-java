package io.github.tcdl.msb.api.message;

import org.apache.commons.lang3.Validate;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class Topics {

    private final String to;
    private final String response;
    private final String forward;

    @JsonCreator
    public Topics(@JsonProperty("to") String to, @JsonProperty("response") String response,  @JsonProperty("forward") String forward) {
        Validate.notNull(to, "the 'to' must not be null");
        this.to = to;
        this.response = response;
        this.forward = forward;
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

    @Override
    public String toString() {
        return "Topics [to=" + to + ", response=" + response + ", forward=" + response + "]";
    }
}
