package io.github.tcdl.messages;

import java.util.Date;

import io.github.tcdl.ServiceDetails;

/**
 * Created by rdro on 4/22/2015.
 */
public class MetaMessage {

    private Integer ttl;
    private Date createdAt;
    private Long durationMs;
    private ServiceDetails serviceDetails;

    public MetaMessage withTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }

    public MetaMessage withCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    public MetaMessage withDurationMs(Long durationMs) {
        this.durationMs = durationMs;
        return this;
    }

    public MetaMessage withServiceDetails(ServiceDetails serviceDetails) {
        this.serviceDetails = serviceDetails;
        return this;
    }

    public Integer getTtl() {
        return ttl;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public Long getDurationMs() {
        return durationMs;
    }

    public ServiceDetails getServiceDetails() {
        return serviceDetails;
    }
}
