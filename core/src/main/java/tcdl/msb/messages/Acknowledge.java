package tcdl.msb.messages;

/**
 * Created by rdro on 4/22/2015.
 */
public class Acknowledge {

    private String responderId;
    private Integer responsesRemaining;
    private Integer timeoutMs;

    public Acknowledge withResponderId(String responderId) {
        this.responderId = responderId;
        return this;
    }

    public Acknowledge withResponsesRemaining(Integer responsesRemaining) {
        this.responsesRemaining = responsesRemaining;
        return this;
    }

    public Acknowledge withTimeoutMs(Integer timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
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
