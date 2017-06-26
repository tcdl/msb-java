package io.github.tcdl.msb.cli;

import io.github.tcdl.msb.api.ExchangeType;

/**
 * Represents msb namespace with specific exchange type.
 */
public class MsbExchange {

    private String namespace;
    private ExchangeType type;

    public MsbExchange(String namespace, ExchangeType type) {
        this.namespace = namespace;
        this.type = type;
    }

    public String getNamespace() {
        return namespace;
    }

    public ExchangeType getType() {
        return type;
    }
}
