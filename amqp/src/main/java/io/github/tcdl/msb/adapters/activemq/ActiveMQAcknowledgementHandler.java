package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;

public class ActiveMQAcknowledgementHandler implements AcknowledgementHandlerInternal {
    @Override
    public void autoConfirm() {

    }

    @Override
    public void autoReject() {

    }

    @Override
    public void autoRetry() {

    }

    @Override
    public void setAutoAcknowledgement(boolean autoAcknowledgement) {

    }

    @Override
    public boolean isAutoAcknowledgement() {
        return false;
    }

    @Override
    public void confirmMessage() {

    }

    @Override
    public void retryMessage() {

    }

    @Override
    public void retryMessageFirstTime() {

    }

    @Override
    public void rejectMessage() {

    }
}
