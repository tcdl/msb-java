package io.github.tcdl.msb.message;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.Message.Builder;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.config.ServiceDetails;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;
import io.github.tcdl.msb.support.IncrementingClock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class MessageFactoryTest {

    private final Instant FIXED_CLOCK_INSTANT = Instant.parse("2007-12-03T10:15:30.00Z");
    private final Clock FIXED_CLOCK = Clock.fixed(FIXED_CLOCK_INSTANT, ZoneId.systemDefault());

    @Mock
    private MessageTemplate messageOptions;

    @Mock
    private MsbConfig msbConf;

    private ServiceDetails serviceDetails = TestUtils.createMsbConfigurations().getServiceDetails();

    private MessageFactory messageFactory = new MessageFactory(serviceDetails, FIXED_CLOCK, TestUtils.createMessageMapper());

    @Test
    public void testCreateRequestMessageWithPayload() {
        String bodyText = "body text";
        RestPayload requestPayload = TestUtils.createPayloadWithTextBody(bodyText);

        Builder requestMessageBuilder = TestUtils.createMessageBuilder(FIXED_CLOCK);

        Message message = messageFactory.createRequestMessage(requestMessageBuilder, requestPayload);

        TestUtils.assertRawPayloadContainsBodyText(bodyText, message);
        assertNull(message.getAck());
        assertNull(message.getTopics().getForward());
    }

    @Test
    public void testCreateRequestMessageWithoutPayload() {
        Builder requestMessageBuilder = TestUtils.createMessageBuilder(FIXED_CLOCK);

        Message message = messageFactory.createRequestMessage(requestMessageBuilder, null);

        assertNull(message.getRawPayload());
        assertNull(message.getAck());
        assertNull(message.getTopics().getForward());
    }

    @Test
    public void testCreateResponseMessageWithPayloadAndAck() {
        String bodyText = "body text";
        Builder responseMessageBuilder = TestUtils.createMessageBuilder(FIXED_CLOCK);
        RestPayload responsePayload = TestUtils.createPayloadWithTextBody(bodyText);
        Acknowledge ack = new Acknowledge.Builder()
                .withResponderId(Utils.generateId())
                .withResponsesRemaining(3)
                .withTimeoutMs(100)
                .build();

        Message message = messageFactory.createResponseMessage(responseMessageBuilder, ack, responsePayload);

        TestUtils.assertRawPayloadContainsBodyText(bodyText, message);
        assertEquals(ack, message.getAck());
        assertNull(message.getTopics().getForward());
    }

    @Test
    public void testCreateResponseMessageWithoutPayloadAndAck() {
        Builder responseMessageBuilder = TestUtils.createMessageBuilder(FIXED_CLOCK);

        Message message = messageFactory.createResponseMessage(responseMessageBuilder, null, null);

        assertNull(message.getRawPayload());
        assertNull(message.getAck());
        assertNull(message.getTopics().getForward());
    }

    @Test
    public void testBroadcastMessage() {
        String bodyText = "body text";
        RestPayload broadcastPayload = TestUtils.createPayloadWithTextBody(bodyText);
        Builder broadcastMessageBuilder = TestUtils.createMessageBuilder(FIXED_CLOCK);

        Message message = messageFactory.createBroadcastMessage(broadcastMessageBuilder, broadcastPayload);

        TestUtils.assertRawPayloadContainsBodyText(bodyText, message);
        assertNull(message.getAck());
        assertNull(message.getTopics().getForward());
    }

    @Test
    public void testCreateRequestMessageBuilder() {
        String namespace = "test:request-builder";

        Builder requestMessageBuilder = messageFactory.createRequestMessageBuilder(namespace, null, messageOptions, null);
        Message message = requestMessageBuilder.build();

        assertNotNull(message.getCorrelationId());
        assertThat(message.getTopics().getTo(), is(namespace));
        assertThat(message.getTopics().getResponse(), notNullValue());
        assertNull(message.getTopics().getForward());
    }

    @Test
    public void testCreateRequestMessageBuilderWithForward() {
        String namespace = "test:request-builder";

        String forwardNamespace = "test:forward";

        Builder requestMessageBuilder = messageFactory.createRequestMessageBuilder(namespace, forwardNamespace, messageOptions, null);
        Message message = requestMessageBuilder.build();

        assertNotNull(message.getCorrelationId());
        assertThat(message.getTopics().getTo(), is(namespace));
        assertThat(message.getTopics().getResponse(), notNullValue());
        assertThat(message.getTopics().getForward(), is(forwardNamespace));
    }

    @Test
    public void testCreateRequestMessageBuilderWithTags() {
        String namespace = "test:request-builder";
        String[] tags = new String[] {"tag1", "tag2"};
        MessageTemplate messageTemplate = TestUtils.createSimpleMessageTemplate(tags);

        Builder requestMessageBuilder = messageFactory.createRequestMessageBuilder(namespace, null, messageTemplate, null);
        Message message = requestMessageBuilder.build();

        assertArrayEquals(tags, message.getTags().toArray());
    }

    @Test
    public void testCreateRequestMessageBuilderWithUniqueTags() {
        String namespace = "test:request-builder";
        String[] tags = new String[] {"tag1", "tag2", "tag2"};

        MessageTemplate messageTemplate = TestUtils.createSimpleMessageTemplate(tags);

        Builder requestMessageBuilder = messageFactory.createRequestMessageBuilder(namespace, null, messageTemplate, null);
        Message message = requestMessageBuilder.build();

        String[] uniqueTags = Stream.of(tags).distinct().collect(Collectors.toList()).toArray(new String[] {});
        assertArrayEquals(uniqueTags, message.getTags().toArray());
    }

    @Test
    public void testCreateRequestMessageBuilderFromOriginalMessage() {
        String namespace = "test:request-builder";
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload(namespace);

        Builder requestMessageBuilder = messageFactory.createRequestMessageBuilder(namespace, null, messageOptions, originalMessage);
        Message message = requestMessageBuilder.build();

        assertNotEquals(originalMessage.getCorrelationId(), message.getCorrelationId());
        assertThat(message.getTopics().getTo(), is(namespace));
        assertThat(message.getTopics().getResponse(), notNullValue());
        assertNull(message.getTopics().getForward());
    }

    @Test
    public void testCreateResponseMessageBuilder() {
        String namespace = "test:response-builder";
        Message originalMessage = TestUtils.createSimpleResponseMessage(namespace);

        Builder responseMessageBuilder = messageFactory.createResponseMessageBuilder(messageOptions, originalMessage);
        Message message = responseMessageBuilder.build();

        assertThat(message.getCorrelationId(), is(originalMessage.getCorrelationId()));
        assertThat(message.getTopics().getTo(), not(namespace));
        assertThat(message.getTopics().getTo(), not(originalMessage.getTopics().getTo()));
        assertThat(message.getTopics().getTo(), is(originalMessage.getTopics().getResponse()));
        assertThat(message.getTopics().getResponse(), nullValue());
        assertNull(message.getTopics().getForward());
    }

    @Test
    public void testCreateResponseMessageBuilderWithTags() {
        String namespace = "test:response-builder";
        String[] tags = new String[] {"tag1", "tag2"};
        MessageTemplate messageTemplate = TestUtils.createSimpleMessageTemplate(tags);
        Message originalMessage = TestUtils.createSimpleResponseMessage(namespace);

        Builder requestMessageBuilder = messageFactory.createResponseMessageBuilder(messageTemplate, originalMessage);
        Message message = requestMessageBuilder.build();

        assertArrayEquals(tags, message.getTags().toArray());
        assertNull(message.getTopics().getForward());
    }

    @Test
    public void testCreateResponseMessageBuilderWithUniqueTags() {
        String namespace = "test:response-builder";
        String[] tags = new String[] {"tag1", "tag2", "tag2"};
        MessageTemplate messageTemplate = TestUtils.createSimpleMessageTemplate(tags);
        Message originalMessage = TestUtils.createSimpleResponseMessage(namespace);

        Builder requestMessageBuilder = messageFactory.createResponseMessageBuilder(messageTemplate, originalMessage);
        Message message = requestMessageBuilder.build();

        String[] uniqueTags = Stream.of(tags).distinct().collect(Collectors.toList()).toArray(new String[] {});
        assertArrayEquals(uniqueTags, message.getTags().toArray());
    }

    @Test
    public void testBroadcastMessageBuilder() {
        String topic = "topic:target:builder";

        Builder messageBuilder = messageFactory.createBroadcastMessageBuilder(topic, messageOptions);
        Message message = messageBuilder.build();

        assertNotNull(message.getCorrelationId());
        assertEquals(topic, message.getTopics().getTo());
        assertThat(message.getTopics().getResponse(), nullValue());
        assertNull(message.getTopics().getForward());
    }

    @Test
    public void testCreateAckBuilder() throws Exception {
        Acknowledge ack = messageFactory.createAckBuilder().build();
        assertNotNull(ack.getResponderId());
    }

    @Test
    public void testCreateRequestMessageBuilderPublishedAtPresent() {
        String bodyText = "body text";
        RestPayload requestPayload = TestUtils.createPayloadWithTextBody(bodyText);

        Builder requestMessageBuilder = TestUtils.createMessageBuilder(FIXED_CLOCK);

        Message message = messageFactory.createRequestMessage(requestMessageBuilder, requestPayload);

        assertNotNull(message.getMeta().getPublishedAt());
        assertEquals(message.getMeta().getPublishedAt(), FIXED_CLOCK.instant());
    }

    @Test
    public void testCreateRequestMessageBuilderPublishedAtIsAfterCreatedAt() {
        String bodyText = "body text";
        RestPayload requestPayload = TestUtils.createPayloadWithTextBody(bodyText);

        IncrementingClock testClockWithStep = new IncrementingClock(FIXED_CLOCK_INSTANT, ZoneId.systemDefault(), 1, ChronoUnit.SECONDS);

        Builder requestMessageBuilder = TestUtils.createMessageBuilder(testClockWithStep);

        Message message = messageFactory.createRequestMessage(requestMessageBuilder, requestPayload);

        Instant publishedAt = message.getMeta().getPublishedAt();
        Instant createdAt = message.getMeta().getCreatedAt();

        assertTrue(publishedAt.isAfter(createdAt));
    }
}
