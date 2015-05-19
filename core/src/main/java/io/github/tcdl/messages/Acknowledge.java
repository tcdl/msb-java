package io.github.tcdl.messages;

import org.apache.commons.lang3.Validate;

/**
 * Created by rdro on 4/22/2015.
 */
public final class Acknowledge {

    private final String responderId;
    private final Integer responsesRemaining;
    private final Integer timeoutMs;

    private Acknowledge(String responderId, Integer responsesRemaining, Integer timeoutMs) {
        Validate.notNull(responderId, "the 'responderId' must not be null");
        this.responderId = responderId;
        this.responsesRemaining = responsesRemaining;
        this.timeoutMs = timeoutMs;
    }

    public static class AcknowledgeBuilder {
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
}
