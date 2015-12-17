package io.github.tcdl.msb.adapters.mock;

import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;

public class MockAcknowledgementHandler implements AcknowledgementHandlerInternal {
    @Override public void autoConfirm() {

    }

    @Override public void autoReject() {

    }

    @Override public void autoRetry() {

    }

    @Override public void setAutoAcknowledgement(boolean autoAcknowledgement) {

    }

    @Override public boolean isAutoAcknowledgement() {
        return true;
    }

    @Override public void confirmMessage() {

    }

    @Override public void retryMessage() {

    }

    @Override public void rejectMessage() {

    }
}
