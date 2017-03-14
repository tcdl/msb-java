package io.github.tcdl.msb.acknowledge;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class AcknowledgementHandlerImplTest {

    private AcknowledgementHandlerImpl handler;

    @Mock
    private AcknowledgementAdapter acknowledgementAdapter;

    @Before
    public void setUp() {
        handler = getHandler(false);
    }

    @Test
    public void testMessageConfirmed() throws Exception {
        handler.confirmMessage();
        verifySingleConfirm();
    }

    @Test
    public void testMessageRejected() throws Exception {
        handler.rejectMessage();
        verifySingleReject();
    }

    @Test
    public void testMessageRequeued() throws Exception {
        handler.retryMessage();
        verifySingleRetry();
    }

    @Test
    public void testMessageConfirmedWhenAutoAcknowledgementDisabled() throws Exception {
        handler.setAutoAcknowledgement(false);
        handler.confirmMessage();
        verifySingleConfirm();
    }

    @Test
    public void testMessageRejectedWhenAutoAcknowledgementDisabled() throws Exception {
        handler.setAutoAcknowledgement(false);
        handler.rejectMessage();
        verifySingleReject();
    }

    @Test
    public void testMessageRequeuedWhenAutoAcknowledgementDisabled() throws Exception {
        handler.setAutoAcknowledgement(false);
        handler.retryMessage();
        verifySingleRetry();
    }

    @Test
    public void testAutoAcknowledgementChanged() throws Exception {
        assertTrue(handler.isAutoAcknowledgement());
        handler.setAutoAcknowledgement(false);
        assertFalse(handler.isAutoAcknowledgement());
    }

    @Test
    public void testRedeliveredMessageRejected() throws Exception {
        handler = getHandler(true);
        handler.rejectMessage();
        verifySingleReject();
    }

    @Test
    public void testRedeliveredMessageRejectedInsteadOfRetry() throws Exception {
        handler = getHandler(true);
        handler.autoRetry();
        verifySingleReject();
    }

    @Test
    public void testManuallyRetriedMessageNotRejected() throws Exception {
        handler = getHandler(true);
        handler.retryMessage();
        verifySingleRetry();
    }

    @Test
    public void testRedeliveredMessageConfirmed() throws Exception {
        handler = getHandler(true);
        handler.confirmMessage();
        verifySingleConfirm();
    }

    @Test
    public void testOnlyFirstRejectInvoked() throws Exception {
        handler.rejectMessage();
        verifySingleReject();
        submitMultipleConfirmRejectRequests();
        verifySingleReject();
    }

    @Test
    public void testOnlyFirstRetryInvoked() throws Exception {
        handler.retryMessage();
        verifySingleRetry();
        submitMultipleConfirmRejectRequests();
        verifySingleRetry();
    }

    @Test
    public void testOnlyFirstConfirmInvoked() throws Exception {
        handler.confirmMessage();
        verifySingleConfirm();
        submitMultipleConfirmRejectRequests();
        verifySingleConfirm();
    }

    @Test
    public void testAutoConfirmConfirmsMessageOnce() throws Exception {
        handler.autoConfirm();
        verifySingleConfirm();
        submitMultipleAutoConfirmAutoRejectRequests();
        verifySingleConfirm();
    }

    @Test
    public void testAutoRejectRejectsMessageOnce() throws Exception {
        handler.autoReject();
        verifySingleReject();
        submitMultipleAutoConfirmAutoRejectRequests();
        verifySingleReject();
    }

    @Test
    public void testAutoRetryRequeueMessageOnce() throws Exception {
        handler.autoRetry();
        verifySingleRetry();
        submitMultipleAutoConfirmAutoRejectRequests();
        verifySingleRetry();
    }

    @Test
    public void testAutoConfirmIgnoredWhenAutoAcknowledgementDisabled() throws Exception {
        handler.setAutoAcknowledgement(false);
        handler.autoConfirm();
        verifyNoMoreInteractions(acknowledgementAdapter);
    }

    @Test
    public void testAutoRejectIgnoredWhenAutoAcknowledgementDisabled() throws Exception {
        handler.setAutoAcknowledgement(false);
        handler.autoReject();
        verifyNoMoreInteractions(acknowledgementAdapter);
    }

    @Test
    public void testAutoRetryIgnoredWhenAutoAcknowledgementDisabled() throws Exception {
        handler.setAutoAcknowledgement(false);
        handler.autoRetry();
        verifyNoMoreInteractions(acknowledgementAdapter);
    }

    @Test
    public void testAutoRetryRejectRedeliveredMessageOnce() throws Exception {
        handler = getHandler(true);
        handler.autoRetry();
        verifySingleReject();
        submitMultipleAutoConfirmAutoRejectRequests();
        verifySingleReject();
    }

    @Test
    public void testAutoConfirmIgnoredWhenConfirmedByClient() throws Exception {
        handler.confirmMessage();
        verifySingleConfirm();
        handler.autoConfirm();
        verifySingleConfirm();
    }

    @Test
    public void testAutoRejectIgnoredWhenConfirmedByClient() throws Exception {
        handler.confirmMessage();
        verifySingleConfirm();
        handler.autoReject();
        verifySingleConfirm();
    }

    @Test
    public void testAutoConfirmIgnoredWhenRetryByClient() throws Exception {
        handler.retryMessage();
        verifySingleRetry();
        handler.autoConfirm();
        verifySingleRetry();
    }

    @Test
    public void testAutoRejectIgnoredWhenRejectedByClient() throws Exception {
        handler.rejectMessage();
        verifySingleReject();
        handler.autoReject();
        verifySingleReject();
    }

    private void verifySingleConfirm() throws Exception {
        verify(acknowledgementAdapter, times(1)).confirm();
        verifyNoMoreInteractions(acknowledgementAdapter);
    }

    private void verifySingleRetry() throws Exception {
        verify(acknowledgementAdapter, times(1)).retry();
        verifyNoMoreInteractions(acknowledgementAdapter);
    }

    private void verifySingleReject() throws Exception {
        verify(acknowledgementAdapter, times(1)).reject();
        verifyNoMoreInteractions(acknowledgementAdapter);
    }

    private void submitMultipleConfirmRejectRequests() {
        IntStream.range(0, 5).forEach((i) -> {
                handler.confirmMessage();
                handler.retryMessage();
                handler.rejectMessage();
        });
    }

    private void submitMultipleAutoConfirmAutoRejectRequests() {
        IntStream.range(0, 5).forEach((i) -> {
            handler.autoReject();
            handler.autoRetry();
            handler.autoConfirm();
        });
    }

    private AcknowledgementHandlerImpl getHandler(boolean isMessageRedelivered) {
        return new AcknowledgementHandlerImpl(acknowledgementAdapter, isMessageRedelivered, "id = 123");
    }

}
