package io.github.tcdl.cli;

public interface CliMessageHandlerSubscriber {
    void subscribe(String topicName, CliMessageHandler handler);
}
