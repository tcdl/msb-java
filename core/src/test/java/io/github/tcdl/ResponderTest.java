package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


/**
 * Created by anstr on 5/26/2015.
 */
public class ResponderTest {
    private MsbMessageOptions config;
    private static final String TOPIC = "test:responder";

    @Mock
    private MsbContext msbContext;

    @Before
    public void setUp() {
        this.config = TestUtils.createSimpleConfig();
        this.msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test
    public void testResponderConstructorOk() {
        MsbContext context = new MsbContext.MsbContextBuilder().build();
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        new Responder(config, originalMessage, context);
    }

    @Test
    public void testProducerWasCreatedForProperTopic() {
        ChannelManager mockChannelManager = mock(ChannelManager.class);
        MsbContext msbContextSpy = spy(msbContext);
        Producer producer = mock(Producer.class);

        when(msbContextSpy.getChannelManager()).thenReturn(mockChannelManager);
        when(mockChannelManager.findOrCreateProducer(anyString())).thenReturn(producer);

        Payload payload = new Payload.PayloadBuilder().build();
        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        Responder responder = new Responder(config, originalMessage, msbContextSpy);

        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        responder.send(payload, null);

        verify(mockChannelManager).findOrCreateProducer(argument.capture());

        assertEquals(originalMessage.getTopics().getResponse(), argument.getValue());
    }
}
