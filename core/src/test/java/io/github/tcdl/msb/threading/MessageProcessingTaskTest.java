package io.github.tcdl.msb.threading;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import io.github.tcdl.msb.support.TestUtils;

import io.github.tcdl.msb.MessageHandler;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.function.Supplier;

import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerImpl;
import io.github.tcdl.msb.api.message.Message;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.MDC;

@RunWith(MockitoJUnitRunner.class)
public class MessageProcessingTaskTest {
    private final String MDC_KEY = "key";
    private final String MDC_VALUE = "any";

    private Message message;

    @Mock
    private MessageHandler mockMessageHandler;

    @Mock
    private AcknowledgementHandlerImpl mockAcknowledgementHandler;

    private MessageProcessingTask task;

    @Before
    public void setUp() {
        message = TestUtils.createSimpleRequestMessage("any");
        task = new MessageProcessingTask(mockMessageHandler, message, mockAcknowledgementHandler);
    }

    @Test
    public void testMessageProcessing() throws IOException {
        task.run();
        verify(mockMessageHandler).handleMessage(any(), eq(mockAcknowledgementHandler));
        verify(mockAcknowledgementHandler, times(1)).autoConfirm();
        verifyNoMoreInteractions(mockAcknowledgementHandler);
    }

    @Test
    public void testExceptionDuringProcessing() {
        testThrowableDuringProcessing(RuntimeException::new);
    }

    @Test
    public void testErrorDuringProcessing() {
        testThrowableDuringProcessing(AssertionError::new);
    }

    private <T extends Throwable> void testThrowableDuringProcessing(Supplier<T> throwable) {
        doThrow(throwable.get()).when(mockMessageHandler).handleMessage(any(), any());

        try {
            task.run();
            verify(mockAcknowledgementHandler, times(1)).autoRetry();
            verifyNoMoreInteractions(mockAcknowledgementHandler);
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testMdcProvided() throws Exception {
        try(Closeable mdcCloseable = MDC.putCloseable(MDC_KEY, MDC_VALUE)) {
            assertTrue("MDC data is missing in a thread while was provided", isMdcPresentInThread());
        }
    }

    @Test
    public void testMdcNotProvided() throws Exception {
        assertFalse("MDC data is present in a thread while was not provided", isMdcPresentInThread());
    }

    private boolean isMdcPresentInThread() throws Exception{
        CompletableFuture<Boolean> isMdcPresentInTaskRun = new CompletableFuture<>();
        CompletableFuture<Boolean> isMdcPresentInOtherRun = new CompletableFuture<>();
        MessageHandler mdcMessageHandler = (message, acknowledgeHandler) -> {
            isMdcPresentInTaskRun.complete(isMdcPresent());
        };
        MessageProcessingTask mdcTask =
                new MessageProcessingTask(mdcMessageHandler, message, mockAcknowledgementHandler);
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        singleThreadExecutor.execute(mdcTask);
        singleThreadExecutor.execute(() -> isMdcPresentInOtherRun.complete(isMdcPresent()));
        assertFalse("MDC data cleanup was not performed", isMdcPresentInOtherRun.get());
        return isMdcPresentInTaskRun.get();
    }

    private boolean isMdcPresent() {
        return MDC_VALUE.equals(MDC.get(MDC_KEY));
    }
}