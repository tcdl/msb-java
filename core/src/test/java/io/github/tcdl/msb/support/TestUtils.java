package io.github.tcdl.msb.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.exception.JsonSchemaValidationException;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.api.message.Topics;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.collector.CollectorManagerFactory;
import io.github.tcdl.msb.collector.TimeoutManager;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.impl.ObjectFactoryImpl;
import io.github.tcdl.msb.message.MessageFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by rdro on 4/28/2015.
 */
public class TestUtils {

    public static MsbContextImpl createSimpleMsbContext() {
        return new TestMsbContextBuilder().build();
    }

    public static TestMsbContextBuilder createMsbContextBuilder() {
        return new TestMsbContextBuilder();
    }

    public static String getSimpleNamespace() {
        return "test:general";
    }

    public static RequestOptions createSimpleRequestOptions() {
        return new RequestOptions.Builder()
                .withMessageTemplate(createSimpleMessageTemplate())
                .build();
    }

    public static RequestOptions createSimpleRequestOptionsWithTags(String... tags) {
        MessageTemplate messageTemplate = createSimpleMessageTemplate();
        messageTemplate.withTags(tags);
        return new RequestOptions.Builder()
                .withMessageTemplate(messageTemplate)
                .build();
    }

    public static MessageTemplate createSimpleMessageTemplate(String... tags) {
        return new MessageTemplate().withTags(tags);
    }

    public static Message createSimpleRequestMessage(String namespace) {
        return createMsbRequestMessage(namespace, null, createSimpleRequestPayload());
    }

    public static Message createSimpleRequestMessageWithTags(String namespace, String... tags) {
        return createMsbRequestMessage(namespace, null, createSimpleRequestPayload(), tags);
    }

    public static Message createSimpleResponseMessage(String namespace) {
        return createMsbRequestMessage(namespace, null, createSimpleResponsePayload());
    }

    public static Message createMsbRequestMessageNoPayload(String namespace) {
        MsbConfig msbConf = createMsbConfigurations();
        return createMsbRequestMessageNoPayload(namespace, namespace + ":response:" +
                msbConf.getServiceDetails().getInstanceId());
    }

    public static Message createMsbBroadcastMessageNoPayload(String namespace) {
        return createMsbRequestMessageNoPayload(namespace, null);
    }

    public static Message createMsbRequestMessageNoPayload(String namespace, String replyTopic) {
        MsbConfig msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(namespace, replyTopic, null);

        MetaMessage.Builder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.Builder()
                .withCorrelationId(Utils.generateId())
                .withId(Utils.generateId())
                .withTopics(topic)
                .withMetaBuilder(metaBuilder)
                .withPayload(null)
                .build();
    }

    public static Message createMsbRequestMessageWithCorrelationId(String topicTo, String correlationId, RestPayload payload) {
        ObjectMapper payloadMapper = createMessageMapper();
        JsonNode payloadNode = Utils.convert(payload, JsonNode.class, payloadMapper);
        return createMsbRequestMessage(topicTo, null, correlationId, payloadNode);
    }

    public static Message createMsbRequestMessageWithCorrelationId(String topicTo, String correlationId, String payloadString) {
        try {
            ObjectMapper payloadMapper = createMessageMapper();
            JsonNode payloadNode = payloadMapper.readValue(String.format("{\"body\": \"%s\" }", payloadString), JsonNode.class);
            return createMsbRequestMessage(topicTo, null, correlationId, payloadNode);
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare request message");
        }
    }

    public static Message createMsbRequestMessage(String topicTo, String payloadString) {
        try {
            ObjectMapper payloadMapper = createMessageMapper();
            JsonNode payloadNode = payloadMapper.readValue(String.format("{\"body\": \"%s\" }", payloadString), JsonNode.class);
            return createMsbRequestMessage(topicTo, null, null, payloadNode);
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare request message");
        }
    }

    public static Message createMsbRequestMessage(String topicTo, String instanceId, RestPayload payload, String... tags) {
        return createMsbRequestMessage(topicTo, instanceId, null, payload, tags);
    }

    public static Message createMsbRequestMessage(String topicTo, String instanceId, String correlationId, RestPayload payload, String... tags) {
        ObjectMapper payloadMapper = createMessageMapper();
        JsonNode payloadNode = Utils.convert(payload, JsonNode.class, payloadMapper);
        return createMsbRequestMessage(topicTo, instanceId, correlationId, payloadNode, tags);
    }

    private static Message createMsbRequestMessage(String topicTo, String instanceId, String correlationId, JsonNode payloadNode, String... tags) {
        MsbConfig msbConf = createMsbConfigurations(instanceId);
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId(), null);
        MetaMessage.Builder metaBuilder = createSimpleMetaBuilder(msbConf, clock);

        Message.Builder builder = new Message.Builder()
                .withCorrelationId(Utils.ifNull(correlationId, Utils.generateId()))
                .withId(Utils.generateId())
                .withTopics(topic)
                .withMetaBuilder(metaBuilder)
                .withPayload(payloadNode);

        if (tags != null) {
            builder.withTags(Arrays.asList(tags));
        }

        return builder.build();
    }

    public static RestPayload<Object, Object, Object, String> createPayloadWithTextBody(String bodyText) {
        return new RestPayload.Builder<Object, Object, Object, String>()
                .withBody(bodyText)
                .build();
    }

    public static Message createMsbResponseMessageWithAckNoPayload(String topicTo) {
        Acknowledge simpleAck = new Acknowledge.Builder().withResponderId(Utils.generateId()).build();
        return createMsbResponseMessageWithAckNoPayload(simpleAck, topicTo, Utils.generateId());
    }

    public static Message createMsbResponseMessageWithAckNoPayload(Acknowledge ack, String topicTo, String correlationId) {
        MsbConfig msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, null, null);
        MetaMessage.Builder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.Builder()
                .withCorrelationId(Utils.ifNull(correlationId, Utils.generateId()))
                .withId(Utils.generateId())
                .withTopics(topic)
                .withMetaBuilder(metaBuilder)
                .withPayload(null)
                .withAck(ack)
                .build();
    }

    public static Message createMsbResponseMessage(Acknowledge ack, JsonNode payload, String topicTo, String correlationId) {
        MsbConfig msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, null, null);
        MetaMessage.Builder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.Builder()
                .withCorrelationId(Utils.ifNull(correlationId, Utils.generateId()))
                .withId(Utils.generateId())
                .withTopics(topic)
                .withMetaBuilder(metaBuilder)
                .withPayload(payload)
                .withAck(ack)
                .build();
    }

    public static Message.Builder createMessageBuilder(Clock clock) {
        MsbConfig msbConf = createMsbConfigurations();

        Topics topic = new Topics("", "", null);
        MetaMessage.Builder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.Builder()
                .withCorrelationId(Utils.generateId())
                .withId(Utils.generateId())
                .withTopics(topic)
                .withMetaBuilder(metaBuilder);
    }

    public static MsbConfig createMsbConfigurations(String instanceId) {
        Config config = ConfigFactory.load();
        if (instanceId != null) {
            config = config.withValue("msbConfig.serviceDetails.instanceId", ConfigValueFactory.fromAnyRef(instanceId));
        }
        return new MsbConfig(config);
    }

    public static MsbConfig createMsbConfigurations() {
        return new MsbConfig(ConfigFactory.load());
    }

    public static ObjectMapper createMessageMapper() {
        return new MsbContextBuilder().createMessageEnvelopeMapper();
    }

    public static RestPayload<Object, Map<String, String>, Object, Map<String, String>> createSimpleRequestPayload() {
        Map<String, String> headers = new HashMap<>();
        headers.put("url", "http://mock/request");
        headers.put("method", "Request");

        Map<String, String> body = new HashMap<>();

        body.put("body", "someRequestBody created at " + Clock.systemDefaultZone().millis());

        return new RestPayload.Builder<Object, Map<String, String>, Object, Map<String, String>>()
                .withHeaders(headers)
                .withBody(body)
                .withBodyBuffer(new byte[5])
                .build();
    }

    public static RestPayload<Object, Map<String, String>, Object, Map<String, String>> createSimpleRequestPayloadWithBodyBuffer(byte [] bodyBuffer) {
        Map<String, String> headers = new HashMap<>();
        headers.put("url", "http://mock/request");
        headers.put("method", "APPLICATION/OCTET-STREAM");

        return new RestPayload.Builder<Object, Map<String, String>, Object, Map<String, String>>()
                .withHeaders(headers)
                .withBodyBuffer(bodyBuffer)
                .build();
    }

    public static RestPayload<Object, Map<String, String>, Object, Map<String, String>> createSimpleResponsePayload() {
        Map<String, String> headers = new HashMap<>();
        headers.put("statusCode", "200");
        headers.put("method", "Response");

        Map<String, String> body = new HashMap<>();
        body.put("body", "someResponseBody");

        return new RestPayload.Builder<Object, Map<String, String>, Object, Map<String, String>>()
                .withHeaders(headers)
                .withBody(body)
                .build();
    }

    public static MetaMessage.Builder createSimpleMetaBuilder(MsbConfig msbConf, Clock clock) {
        return new MetaMessage.Builder(null, clock.instant(), msbConf.getServiceDetails(), clock);
    }

    public static void assertRawPayloadContainsBodyText(String bodyText, Message message) {
        assertNotNull(message.getRawPayload());
        assertTrue(message.getRawPayload().has("body"));
        assertEquals(bodyText, message.getRawPayload().get("body").asText());
    }

    public static void assertRawPayload(String expectedPayload, Message message) {
        assertNotNull(message.getRawPayload());
        assertEquals(expectedPayload, message.getRawPayload().asText());
    }

    public static void assertJsonContains(JsonNode jsonObject, String field, String value) {
        assertTrue(jsonObject.has(field));
        assertNotNull(jsonObject.get(field));
        assertEquals(value, jsonObject.get(field).asText());
    }

    public static void assertRequestMessagePayload(String json, RestPayload payload, String requestNamespace) {
        try {
            MsbContextImpl msbContext = TestUtils.createMsbContextBuilder().build();
            new JsonValidator().validate(json, msbContext.getMsbConfig().getSchema());
            ObjectMapper payloadMapper = msbContext.getPayloadMapper();
            JsonNode jsonObject = payloadMapper.readTree(json);

            // payload fields set
            assertTrue("Message not contain 'body' field", jsonObject.get("payload").has("body"));
            assertTrue("Message not contain 'headers' field", jsonObject.get("payload").has("headers"));

            // payload fields match sent
            assertEquals("Message 'body' is incorrect", payloadMapper.writeValueAsString(payload.getBody()),
                    jsonObject.get("payload").get("body").toString());
            assertEquals("Message 'bodyBuffer' is incorrect", payloadMapper.writeValueAsString(payload.getBodyBuffer()),
                    jsonObject.get("payload").get("bodyBuffer").toString());
            assertEquals("Message 'headers' is incorrect", payloadMapper.writeValueAsString(payload.getHeaders()), jsonObject
                    .get("payload").get("headers").toString());

            // topics
            TestUtils.assertJsonContains(jsonObject.get("topics"), "to", requestNamespace);
            TestUtils.assertJsonContains(jsonObject.get("topics"), "response", requestNamespace + ":response:"
                    + msbContext.getMsbConfig().getServiceDetails().getInstanceId());

        } catch (JsonSchemaValidationException | IOException e) {
            fail("Message validation failed");
        }
    }

    public static void assertResponseMessagePayload(String json, RestPayload originalResponsePayload, String responseNamespace) {
        try {
            MsbContextImpl msbContext = TestUtils.createMsbContextBuilder().build();
            new JsonValidator().validate(json, msbContext.getMsbConfig().getSchema());
            ObjectMapper payloadMapper = msbContext.getPayloadMapper();
            JsonNode jsonObject = payloadMapper.readTree(json);

            // payload fields set
            assertTrue("Message not contain 'body' filed", jsonObject.get("payload").has("body"));
            assertTrue("Message not contain 'headers' filed", jsonObject.get("payload").has("headers"));

            // payload fields match sent
            assertEquals("Message 'body' is incorrect", payloadMapper.writeValueAsString(originalResponsePayload.getBody()),
                    jsonObject.get("payload").get("body").toString());
            assertEquals("Message 'headers' is incorrect", payloadMapper.writeValueAsString(originalResponsePayload.getHeaders()),
                    jsonObject.get("payload").get("headers").toString());

            // topics
            TestUtils.assertJsonContains(jsonObject.get("topics"), "to", responseNamespace);
            assertFalse(jsonObject.get("topics").has("response"));
        } catch (JsonSchemaValidationException | IOException e) {
            fail("Message validation failed");
        }
    }

    public static void assertMessageTags(String json, String... tags) {
        try {
            MsbContextImpl msbContext = TestUtils.createMsbContextBuilder().build();
            new JsonValidator().validate(json, msbContext.getMsbConfig().getSchema());
            JsonNode jsonObject = msbContext.getPayloadMapper().readTree(json);
            assertTrue("Message does not contain 'tags' field", jsonObject.has("tags"));
            JsonNode tagsNode = jsonObject.get("tags");
            for (int i = 0; i < tagsNode.size(); i++) {
                assertEquals(tags[i], tagsNode.get(i).asText());
            }
        } catch (Exception e) {
            fail("Message validation failed");
        }
    }

    public static class TestMsbContextBuilder {
        private Optional<MsbConfig> msbConfigOp = Optional.empty();
        private Optional<MessageFactory> messageFactoryOp = Optional.empty();
        private Optional<ChannelManager> channelManagerOp = Optional.empty();
        private Optional<Clock> clockOp = Optional.empty();
        private Optional<TimeoutManager> timeoutManagerOp = Optional.empty();
        private Optional<ObjectFactory> objectFactoryOp = Optional.empty();
        private Optional<CollectorManagerFactory> collectorManagerFactoryOp = Optional.empty();

        public TestMsbContextBuilder withMsbConfigurations(MsbConfig msbConfig) {
            this.msbConfigOp = Optional.ofNullable(msbConfig);
            return this;
        }

        public TestMsbContextBuilder withMessageFactory(MessageFactory messageFactory) {
            this.messageFactoryOp = Optional.ofNullable(messageFactory);
            return this;
        }

        public TestMsbContextBuilder withChannelManager(ChannelManager channelManager) {
            this.channelManagerOp = Optional.ofNullable(channelManager);
            return this;
        }

        public TestMsbContextBuilder withClock(Clock clock) {
            this.clockOp = Optional.ofNullable(clock);
            return this;
        }

        public TestMsbContextBuilder withTimeoutManager(TimeoutManager timeoutManager) {
            this.timeoutManagerOp = Optional.ofNullable(timeoutManager);
            return this;
        }

        public TestMsbContextBuilder withObjectFactory(ObjectFactory objectFactory) {
            this.objectFactoryOp = Optional.ofNullable(objectFactory);
            return this;
        }

        public TestMsbContextBuilder withCollectorManagerFactory(CollectorManagerFactory collectorManagerFactory) {
            this.collectorManagerFactoryOp = Optional.ofNullable(collectorManagerFactory);
            return this;
        }

        public MsbContextImpl build() {
            MsbConfig msbConfig = msbConfigOp.orElse(TestUtils.createMsbConfigurations());
            Clock clock = clockOp.orElse(Clock.systemDefaultZone());
            ObjectMapper messageMapper = createMessageMapper();
            ChannelManager channelManager = channelManagerOp.orElseGet(() -> new ChannelManager(msbConfig, clock, new JsonValidator(), messageMapper));
            MessageFactory messageFactory = messageFactoryOp.orElseGet(() -> new MessageFactory(msbConfig.getServiceDetails(), clock, messageMapper));
            TimeoutManager timeoutManager = timeoutManagerOp.orElseGet(() -> new TimeoutManager(1));
            CollectorManagerFactory collectorManagerFactory = collectorManagerFactoryOp.orElseGet(() -> new CollectorManagerFactory(channelManager));
            TestMsbContext msbContext = new TestMsbContext(msbConfig, messageFactory, channelManager, clock, timeoutManager, collectorManagerFactory);

            ObjectFactory objectFactory = objectFactoryOp.orElseGet(() -> new ObjectFactoryImpl(msbContext));
            msbContext.setFactory(objectFactory);
            return msbContext;
        }

        private static class TestMsbContext extends MsbContextImpl {
            TestMsbContext(MsbConfig msbConfig, MessageFactory messageFactory,
                    ChannelManager channelManager, Clock clock, TimeoutManager timeoutManager, CollectorManagerFactory collectorManagerFactory) {
                super(msbConfig, messageFactory, channelManager, clock, timeoutManager, createMessageMapper(), collectorManagerFactory);
            }

            public void setFactory(ObjectFactory objectFactory) {
                super.setObjectFactory(objectFactory);
            }
        }
    }
}