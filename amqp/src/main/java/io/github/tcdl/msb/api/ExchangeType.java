package io.github.tcdl.msb.api;

public enum ExchangeType {

    FANOUT,
    TOPIC,
    X_CONSISTENT_HASH(){
        @Override
        public String value() {
            return "x-consistent-hash";
        }
    };

    public String value(){
        return this.name().toLowerCase();
    }
}
