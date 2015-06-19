package io.github.tcdl.config;

/**
 * Created by rdro on 4/22/2015.
 */
public class MsbMessageOptions {

    private String namespace;
    private Integer ackTimeout;
    private Integer responseTimeout;
    private Integer waitForResponses;
    private Integer ttl;

    public String getNamespace() {
        return namespace;
    }

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

    public Integer getTtl() {
        return ttl;
    }

    public boolean isWaitForResponses() {
        return getWaitForResponses() != 0;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
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

    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "MsbMessageOptions [namespace=" + namespace + ", ackTimeout="
                + ackTimeout + ", responseTimeout=" + responseTimeout
                + ", waitForResponses=" + waitForResponses + ", ttl=" + ttl
                + "]";
    }

}
