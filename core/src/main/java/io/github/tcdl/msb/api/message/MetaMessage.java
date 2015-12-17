package io.github.tcdl.msb.api.message;

import io.github.tcdl.msb.config.ServiceDetails;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.apache.commons.lang3.Validate;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class MetaMessage {

    private final Integer ttl;
    private final Instant createdAt;
    private final Instant publishedAt;
    private final Long durationMs;
    private final ServiceDetails serviceDetails;

    private MetaMessage(@JsonProperty("ttl") Integer ttl, @JsonProperty("createdAt") Instant createdAt, @JsonProperty("publishedAt") Instant publishedAt,
            @JsonProperty("durationMs") Long durationMs, @JsonProperty("serviceDetails") ServiceDetails serviceDetails) {
        Validate.notNull(createdAt, "the 'createdAt' must not be null");
        Validate.notNull(serviceDetails, "the 'serviceDetails' must not be null");
        this.ttl = ttl;
        this.createdAt = createdAt;
        this.publishedAt = publishedAt;
        this.durationMs = durationMs;
        this.serviceDetails = serviceDetails;
    }

    public static class Builder {
        private Integer ttl;
        private Instant createdAt;
        private Instant publishedAt;
        private ServiceDetails serviceDetails;
        private Clock clock;

        public Builder(Integer ttl, Instant createdAt, ServiceDetails serviceDetails, Clock clock) {
            this.ttl = ttl;
            this.createdAt = createdAt;
            this.serviceDetails = serviceDetails;
            this.clock = clock;
        }     

        public MetaMessage build() {
            publishedAt = clock.instant();
            Long durationMs = Duration.between(this.createdAt, publishedAt).toMillis();;
            return new MetaMessage(ttl, createdAt, publishedAt, durationMs, serviceDetails);
        }
    }

    public Integer getTtl() {
        return ttl;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public Long getDurationMs() {
        return durationMs;
    }

    public ServiceDetails getServiceDetails() {
        return serviceDetails;
    }

    @Override
    public String toString() {
        return "MetaMessage [ttl=" + ttl + ", createdAt=" + createdAt + ", durationMs=" + durationMs + ", serviceDetails=" + serviceDetails + "]";
    }    
}