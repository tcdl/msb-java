package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.Channel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AmqpAcknowledgementAdapterTest {
    private final static String MESSAGE_TEXT_ID = "id = 123";
    private final static long DELIVERY_TAG = 12337564;

    private AmqpAcknowledgementAdapter adapter;

    @Mock
    private Channel channel;

    @Before
    public void setUp() {
        adapter = new AmqpAcknowledgementAdapter(channel, MESSAGE_TEXT_ID, DELIVERY_TAG);
    }

    @Test
    public void testConfirmSuccess() throws Exception {
        adapter.confirm();
        verify(channel, times(1)).basicAck(DELIVERY_TAG, false);
    }

    @Test
    public void testRejectSuccess() throws Exception {
        adapter.reject();
        verify(channel, times(1)).basicReject(DELIVERY_TAG, false);
    }

    @Test
    public void testRetrySuccess() throws Exception {
        adapter.retry();
        verify(channel, times(1)).basicReject(DELIVERY_TAG, true);
    }

}
