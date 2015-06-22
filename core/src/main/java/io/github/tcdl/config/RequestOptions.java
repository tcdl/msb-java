package io.github.tcdl.config;

/**
 * Options to configure a requester to wait for acknowledges or responses
 *
 * Created by rdrozdov-tc on 6/22/15.
 */
public class RequestOptions {

    private Integer ackTimeout;
    private Integer responseTimeout;
    private Integer waitForResponses;
    private MessageTemplate messageTemplate;

    public Integer getAckTimeout() {
        return ackTimeout;
    }

    public Integer getResponseTimeout() {
        return responseTimeout;
    }

    public Integer getWaitForResponses() {
        if (waitForResponses == null || waitForResponses == -1) {
            return 0;
        }
        return waitForResponses;
    }

    public boolean isWaitForResponses() {
        return getWaitForResponses() != 0;
    }

    public void setAckTimeout(Integer ackTimeout) {
        this.ackTimeout = ackTimeout;
    }

    public void setResponseTimeout(Integer responseTimeout) {
        this.responseTimeout = responseTimeout;
    }

    public void setWaitForResponses(Integer waitForResponses) {
        this.waitForResponses = waitForResponses;
    }

    public MessageTemplate getMessageTemplate() {
        return messageTemplate;
    }

    public void setMessageTemplate(MessageTemplate messageTemplate) {
        this.messageTemplate = messageTemplate;
    }

    @Override
    public String toString() {
        return "RequestOptions [ackTimeout=" + ackTimeout
                + ", responseTimeout=" + responseTimeout
                + ", waitForResponses=" + waitForResponses
                + (messageTemplate != null ? messageTemplate : "")
                + "]";
    }

}
