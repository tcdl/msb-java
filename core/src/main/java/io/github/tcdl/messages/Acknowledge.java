package io.github.tcdl.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.Validate;

/**
 * Created by rdro on 4/22/2015.
 */
public final class Acknowledge {

    private final String responderId;
    private final Integer responsesRemaining;
    private final Integer timeoutMs;

    @JsonCreator
    private Acknowledge(@JsonProperty("responderId") String responderId,
            @JsonProperty("responsesRemaining") Integer responsesRemaining,
            @JsonProperty("timeoutMs") Integer timeoutMs) {
        Validate.notNull(responderId, "the 'responderId' must not be null");
        this.responderId = responderId;
        this.responsesRemaining = responsesRemaining;
        this.timeoutMs = timeoutMs;
    }

    public static class AcknowledgeBuilder {
        public Integer getResponsesRemaining() {
            return responsesRemaining;
        }

        public Integer getTimeoutMs() {
            return timeoutMs;
        }

        private String responderId;
        private Integer responsesRemaining;
        private Integer timeoutMs;

        public AcknowledgeBuilder setResponderId(String responderId) {
            this.responderId = responderId;
            return this;
        }

        public AcknowledgeBuilder setResponsesRemaining(Integer responsesRemaining) {
            this.responsesRemaining = responsesRemaining;
            return this;
        }

        public AcknowledgeBuilder setTimeoutMs(Integer timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        public Acknowledge build() {
            return new Acknowledge(responderId, responsesRemaining, timeoutMs);
        }
    }

    public String getResponderId() {
        return responderId;
    }

    public Integer getResponsesRemaining() {
        return responsesRemaining;
    }

    public Integer getTimeoutMs() {
        return timeoutMs;
    }

    @Override
    public String toString() {
        return "Acknowledge [responderId=" + responderId +
                ", responsesRemaining=" + responsesRemaining +
                ", timeoutMs=" + timeoutMs + "]";
    }
}
