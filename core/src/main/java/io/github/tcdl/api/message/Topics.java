package io.github.tcdl.api.message;

import org.apache.commons.lang3.Validate;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by rdro on 4/23/2015.
 */
public final class Topics {

    private final String to;
    private final String response;

    @JsonCreator
    public Topics(@JsonProperty("to") String to, @JsonProperty("response") String response) {
        Validate.notNull(to, "the 'to' must not be null");
        this.to = to;
        this.response = response;
    }

    public String getTo() {
        return to;
    }

    public String getResponse() {
        return response;
    }

    @Override
    public String toString() {
        return "Topics [to=" + to + ", response=" + response + "]";
    }
}
