package io.github.tcdl.config;

/**
 * A message template to be used to construct a request/response message
 */
public class MessageTemplate {

    private Integer ttl;

    /**
     *
     * @return time to live for the message
     */
    public Integer getTtl() {
        return ttl;
    }

    /**
     *
     * @param ttl time to live for the message
     */
    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "MessageTemplate [ttl=" + ttl + "]";
    }
}
