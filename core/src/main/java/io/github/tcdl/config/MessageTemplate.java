package io.github.tcdl.config;

/**
 * A message template to be used to construct a request/response message
 *
 * Created by rdro on 4/22/2015.
 */
public class MessageTemplate {

    private Integer ttl;

    public Integer getTtl() {
        return ttl;
    }

    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    @Override
    public String toString() {
        return "MessageTemplate [ttl=" + ttl + "]";
    }
}
