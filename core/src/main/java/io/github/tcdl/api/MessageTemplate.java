package io.github.tcdl.api;

/**
 * A message template to be used to construct a request/response message
 */
public class MessageTemplate {
    
    public MessageTemplate() {
        super();
    }

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
    public MessageTemplate setTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public String toString() {
        return "MessageTemplate [ttl=" + ttl + "]";
    }
}
