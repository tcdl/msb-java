package io.github.tcdl.exception;

public class ConfigurationException extends RuntimeException {

    public ConfigurationException(String mandatoryOption) {
        super(String.format("Mandatory configuration option '%s' is not defined", mandatoryOption));
    }
}
