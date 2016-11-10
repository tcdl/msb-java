package io.github.tcdl.msb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.api.message.Topics;
import io.github.tcdl.msb.collector.ConsumedMessagesAwareMessageHandler;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;
import io.github.tcdl.msb.threading.MessageHandlerInvoker;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.MDC;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerTest {

    private static final String TOPIC = "test:consumer";

    private static final String MDC_KEY_TAGS = "msbTags";

    private static final String MDC_KEY_CORR_ID = "msbCorrelationId";

    private static final String MDC_SPLIT_KEY = "tagkey";

    private static final String MDC_SPLIT_BY = ":";

    private static final String CORRELATION_ID = "34223432423423";

    @Mock
    private ConsumerAdapter adapterMock;

    @Mock
    private MsbConfig msbConfMock;

    @Mock
    private MessageHandler messageHandlerMock;

    @Mock
    private ConsumedMessagesAwareMessageHandler consumedMessagesAwareMessageHandlerMock;

    @Mock
    private MessageHandlerResolver messageHandlerResolverMock;

    @Mock
    private MessageHandlerResolver consumedMessagesAwareMessageHandlerResolverMock;

    @Mock
    private MessageHandlerInvoker messageHandlerInvokerMock;

    @Mock
    private AcknowledgementHandlerInternal acknowledgementHandlerMock;

    private Clock clock = Clock.systemDefaultZone();

    private JsonValidator validator = new JsonValidator();

    private ObjectMapper messageMapper = TestUtils.createMessageMapper();

    @Before
    public void setUp() {
        when(messageHandlerResolverMock.resolveMessageHandler(any()))
                .thenReturn(Optional.of(messageHandlerMock));

        when(consumedMessagesAwareMessageHandlerResolverMock.resolveMessageHandler(any()))
                .thenReturn(Optional.of(consumedMessagesAwareMessageHandlerMock));

        when(msbConfMock.getMdcLoggingKeyCorrelationId()).thenReturn(MDC_KEY_CORR_ID);
        when(msbConfMock.getMdcLoggingKeyMessageTags()).thenReturn(MDC_KEY_TAGS);
        when(msbConfMock.isMdcLogging()).thenReturn(true);
        when(msbConfMock.getMdcLoggingSplitTagsBy()).thenReturn(MDC_SPLIT_BY);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullAdapter() {
        new Consumer(null, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullTopic() {
        new Consumer(adapterMock, messageHandlerInvokerMock, null, messageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMessageHandler() {
        new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, null, msbConfMock, clock, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMsbConf() {
        new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, null, clock, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullClock() {
        new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, null, validator, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullValidator() {
        new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, clock, null, messageMapper);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullMessageMapper() {
        new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, clock, validator, null);
    }

    @Test
    public void testValidMessageProcessedBySubscriber() throws JsonConversionException {
        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(originalMessage, messageMapper), acknowledgementHandlerMock);

        verifyMessageHandled();
    }

    @Test
    public void testConsumedMessagesAwareMessageHandlerNotifiedWhenMessageHandled() throws JsonConversionException {
        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, consumedMessagesAwareMessageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(originalMessage, messageMapper), acknowledgementHandlerMock);

        verify(consumedMessagesAwareMessageHandlerMock, times(1)).notifyMessageConsumed();
    }

    @Test
    public void testConsumedMessagesAwareMessageHandlerNotifiedWhenMessageLost() throws JsonConversionException {
        doThrow(new RuntimeException("Something really unexpected.")).when(messageHandlerInvokerMock).execute(any(), any(), any());

        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, consumedMessagesAwareMessageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(originalMessage, messageMapper), acknowledgementHandlerMock);

        verify(consumedMessagesAwareMessageHandlerMock, times(1)).notifyMessageConsumed();
        verify(consumedMessagesAwareMessageHandlerMock, times(1)).notifyConsumedMessageIsLost();
    }

    @Test
    public void testMessageHandlerCantBeResolved() throws JsonConversionException {
        when(messageHandlerResolverMock.resolveMessageHandler(any()))
                .thenReturn(Optional.empty());

        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(originalMessage, messageMapper), acknowledgementHandlerMock);

        verifyMessageNotHandled();
        verify(acknowledgementHandlerMock, times(1)).autoReject();
    }

    @Test
    public void testMessageHandlerInvokeException() throws JsonConversionException {
        doThrow(new RuntimeException("Something really unexpected.")).when(messageHandlerInvokerMock).execute(any(), any(), any());

        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(originalMessage, messageMapper), acknowledgementHandlerMock);

        verify(acknowledgementHandlerMock, times(1)).autoRetry();
    }

    @Test
    public void testExceptionWhileMessageConvertingProcessedBySubscriber() throws JsonConversionException {
        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);

        consumer.handleRawMessage("{\"body\":\"fake message\"}", acknowledgementHandlerMock);

        verifyMessageNotHandled();
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicSkipValidation() {
        MsbConfig msbConf = spy(TestUtils.createMsbConfigurations());

        // disable validation
        when(msbConf.isValidateMessage()).thenReturn(false);

        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);

        // create a message with required empty namespace
        Message message = TestUtils.createSimpleRequestMessage("");

        consumer.handleRawMessage(Utils.toJson(message, messageMapper), acknowledgementHandlerMock);

        // should skip validation and process it
        verifyMessageHandled();
    }


    @Test
    public void testHandleRawMessageConsumeFromTopic() throws JsonConversionException {
        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(originalMessage, messageMapper), acknowledgementHandlerMock);
        verifyMessageHandled();
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicValidateThrowException() {
        MsbConfig msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConf, clock, validator, messageMapper);

        consumer.handleRawMessage("{\"body\":\"fake message\"}", acknowledgementHandlerMock);
        verifyMessageNotHandled();
    }

    @Test
    public void testHandleRawMessageConsumeFromServiceTopicValidateThrowException() {
        String service_topic = "_service:topic";
        MsbConfig msbConf = TestUtils.createMsbConfigurations();
        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, service_topic, messageHandlerResolverMock, msbConf, clock, validator, messageMapper);

        consumer.handleRawMessage("{\"body\":\"fake message\"}", acknowledgementHandlerMock);
        verifyMessageNotHandled();
    }

    @Test
    public void testHandleRawMessageConsumeFromTopicExpiredMessage() throws JsonConversionException {
        Message expiredMessage = createExpiredMsbRequestMessageWithTopicTo(TOPIC);
        Consumer consumer = new Consumer(adapterMock, messageHandlerInvokerMock, TOPIC, messageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(expiredMessage, messageMapper), acknowledgementHandlerMock);
        verifyMessageNotHandled();
    }

    @Test
    public void testSaveMdcSuccessWithTagsSplit() throws JsonConversionException {
        verifyMdc(true, true);
    }

    @Test
    public void testSaveMdcSuccessNoTagsSplit() throws JsonConversionException {
        when(msbConfMock.getMdcLoggingSplitTagsBy()).thenReturn("");
        verifyMdc(true, false);

        when(msbConfMock.getMdcLoggingSplitTagsBy()).thenReturn(null);
        verifyMdc(true, false);
    }

    @Test
    public void testSaveMdcDisabled() throws JsonConversionException {
        when(msbConfMock.isMdcLogging()).thenReturn(false);
        verifyMdc(false, false);
    }

    private void verifyMdc(boolean isMdcExpected, boolean isSplitExpected) {
        String splitTagVal = "tag2" + MDC_SPLIT_BY + "tag2!$#.$#$$#&&**";
        String splitTag = MDC_SPLIT_KEY + MDC_SPLIT_BY + splitTagVal;
        Message originalMessage = TestUtils.createMsbRequestMessage(
                TOPIC, null, CORRELATION_ID, TestUtils.createSimpleRequestPayload(), "tag1", splitTag, "tag3");

        MessageHandlerInvoker testInvokeStrategy = new MessageHandlerInvoker() {

            @Override
            public void execute(MessageHandler messageHandler, Message message, AcknowledgementHandlerInternal acknowledgeHandler) {
                if(isMdcExpected) {
                    assertEquals("tag1,"+splitTag+",tag3", MDC.get(MDC_KEY_TAGS));
                    assertEquals(CORRELATION_ID, MDC.get(MDC_KEY_CORR_ID));

                } else {
                    assertTrue(StringUtils.isEmpty(MDC.get(MDC_KEY_TAGS)));
                    assertTrue(StringUtils.isEmpty(MDC.get(MDC_KEY_CORR_ID)));

                }

                if(isSplitExpected) {
                    assertEquals(splitTagVal, MDC.get(MDC_SPLIT_KEY));
                } else {
                    assertTrue(StringUtils.isEmpty(MDC.get(MDC_SPLIT_KEY)));
                }
            }

            @Override
            public void shutdown() {

            }
        };

        Consumer consumer = new Consumer(adapterMock, testInvokeStrategy, TOPIC, messageHandlerResolverMock, msbConfMock, clock, validator, messageMapper);

        consumer.handleRawMessage(Utils.toJson(originalMessage, messageMapper), acknowledgementHandlerMock);
        Map<String, String> map = MDC.getCopyOfContextMap();
        assertTrue("MDC cleanup was expected but was not performed", map == null || map.isEmpty());
    }

    private  Message createExpiredMsbRequestMessageWithTopicTo(String topicTo) {
        Instant MOMENT_IN_PAST = Instant.parse("2007-12-03T10:15:30.00Z");

        MsbConfig msbConf = new MsbConfig(ConfigFactory.load());
        Clock clock = Clock.fixed(MOMENT_IN_PAST, ZoneId.systemDefault());

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId(), null);
        MetaMessage.Builder metaBuilder = new MetaMessage.Builder(0, clock.instant(), msbConf.getServiceDetails(), clock);
        return new Message.Builder().withCorrelationId(Utils.generateId()).withId(Utils.generateId()).withTopics(topic).withMetaBuilder(metaBuilder)
                .build();
    }

    private void verifyMessageHandled() {
        verify(messageHandlerInvokerMock, times(1)).execute(eq(messageHandlerMock), any(Message.class), eq(acknowledgementHandlerMock));
    }

    private void verifyMessageNotHandled() {
        verify(messageHandlerInvokerMock, never()).execute(any(), any(), any());
    }
}