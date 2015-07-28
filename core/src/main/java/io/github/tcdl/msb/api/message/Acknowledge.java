package io.github.tcdl.msb.api.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.Validate;

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

    public static class Builder {
        private String responderId;
        private Integer responsesRemaining;
        private Integer timeoutMs;

        public Builder withResponderId(String responderId) {
            this.responderId = responderId;
            return this;
        }

        public Builder withResponsesRemaining(Integer responsesRemaining) {
            this.responsesRemaining = responsesRemaining;
            return this;
        }

        public Builder withTimeoutMs(Integer timeoutMs) {
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

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Acknowledge that = (Acknowledge) o;

        if (responderId != null ? !responderId.equals(that.responderId) : that.responderId != null)
            return false;
        if (responsesRemaining != null ? !responsesRemaining.equals(that.responsesRemaining) : that.responsesRemaining != null)
            return false;
        return !(timeoutMs != null ? !timeoutMs.equals(that.timeoutMs) : that.timeoutMs != null);

    }

    @Override public int hashCode() {
        int result = responderId != null ? responderId.hashCode() : 0;
        result = 31 * result + (responsesRemaining != null ? responsesRemaining.hashCode() : 0);
        result = 31 * result + (timeoutMs != null ? timeoutMs.hashCode() : 0);
        return result;
    }

}
