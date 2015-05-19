package io.github.tcdl.messages;

import java.util.Date;
import org.apache.commons.lang3.Validate;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.github.tcdl.config.ServiceDetails;

/**
 * Created by rdro on 4/22/2015.
 */
public final class MetaMessage {

    private final Integer ttl;
    private final Date createdAt;
    private final Long durationMs;
    private final ServiceDetails serviceDetails;

    private MetaMessage(@JsonProperty("ttl") Integer ttl, @JsonProperty("createdAt") Date createdAt, @JsonProperty("durationMs") Long durationMs,
            @JsonProperty("serviceDetails") ServiceDetails serviceDetails) {
        Validate.notNull(createdAt, "the 'createdAt' must not be null");
        Validate.notNull(durationMs, "the 'durationMs' must not be null");
        Validate.notNull(serviceDetails, "the 'serviceDetails' must not be null");
        this.ttl = ttl;
        this.createdAt = createdAt;
        this.durationMs = durationMs;
        this.serviceDetails = serviceDetails;
    }

    public static class MetaMessageBuilder {
        private Integer ttl;
        private Date createdAt;
        private Long durationMs;
        private ServiceDetails serviceDetails;

        public MetaMessageBuilder(Integer ttl, Date createdAt, ServiceDetails serviceDetails) {
            this.ttl = ttl;
            this.createdAt = createdAt;
            this.serviceDetails = serviceDetails;
        }

        public MetaMessageBuilder computeDurationMs() {
            this.durationMs = new Date().getTime() - this.createdAt.getTime();
            return this;
        }

        public MetaMessage build() {
            return new MetaMessage(ttl, createdAt, durationMs, serviceDetails);
        }
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