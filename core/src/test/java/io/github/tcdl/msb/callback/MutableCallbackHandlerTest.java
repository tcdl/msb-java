package io.github.tcdl.msb.callback;

import org.assertj.core.api.exception.RuntimeIOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MutableCallbackHandlerTest {

    @Mock
    private Runnable callback1;

    @Mock
    private Runnable callback2;

    @Mock
    private Runnable callback3;

    @Mock
    private Runnable callback4;

    @InjectMocks
    private MutableCallbackHandler handler;

    @Test
    public void testSuccess() throws Exception {

        doThrow(RuntimeIOException.class).when(callback1).run();

        handler.add(callback1);
        handler.add(callback2);
        handler.add(callback3);

        for(int i=0; i < 11; i++) {
            handler.add(callback4);
        }

        handler.runCallbacks();

        handler.remove(callback2);

        handler.runCallbacks();

        verify(callback1, times(2)).run();
        verify(callback2, times(1)).run();
        verify(callback3, times(2)).run();
        verify(callback4, times(2)).run();
    }

}