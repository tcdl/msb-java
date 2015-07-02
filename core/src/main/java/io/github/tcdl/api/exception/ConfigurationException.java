package io.github.tcdl.api.exception;

public class ConfigurationException extends MsbException {

    public ConfigurationException(String mandatoryOption) {
        super(String.format("Mandatory configuration option '%s' is not defined", mandatoryOption));
    }
}
