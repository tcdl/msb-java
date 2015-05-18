package io.github.tcdl.messages;

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
    private Topics(@JsonProperty("to") String to, @JsonProperty("response") String response) {
        Validate.notNull(to, "the 'to' must not be null");
        this.to = to;
        this.response = response;
    }

    public static class TopicsBuilder {

        private String to;
        private String response;

        public TopicsBuilder setTo(String to) {
            this.to = to;
            return this;
        }

        public TopicsBuilder setResponse(String response) {
            this.response = response;
            return this;
        }

        public Topics build() {
            return new Topics(to, response);
        }
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
