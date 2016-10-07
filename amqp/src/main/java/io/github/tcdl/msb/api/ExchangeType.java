package io.github.tcdl.msb.api;

public enum ExchangeType {

    FANOUT,
    TOPIC;

    public String value(){
        return this.name().toLowerCase();
    }
}
