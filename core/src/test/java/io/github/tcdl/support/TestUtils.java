package io.github.tcdl.support;

import io.github.tcdl.ChannelManager;
import io.github.tcdl.api.MsbContext;
import io.github.tcdl.TimeoutManager;
import io.github.tcdl.api.MessageTemplate;
import io.github.tcdl.api.message.Acknowledge;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.api.RequestOptions;
import io.github.tcdl.api.message.Message;
import io.github.tcdl.api.message.Message.MessageBuilder;
import io.github.tcdl.message.MessageFactory;
import io.github.tcdl.api.message.MetaMessage;
import io.github.tcdl.api.message.MetaMessage.MetaMessageBuilder;
import io.github.tcdl.api.message.Topics;
import io.github.tcdl.api.message.payload.Payload;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.typesafe.config.ConfigFactory;

/**
 * Created by rdro on 4/28/2015.
 */
public class TestUtils {

    public static MsbContext createSimpleMsbContext() {
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

    public static MessageTemplate createSimpleMessageTemplate() {
        MessageTemplate messageTemplate = new MessageTemplate();
        return messageTemplate;
    }

    public static Message createMsbRequestMessageWithPayloadAndTopicTo(String topicTo) {
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().withCorrelationId(Utils.generateId()).setId(Utils.generateId()).withTopics(topic).withMetaBuilder(metaBuilder)
                .withPayload(createSimpleRequestPayload()).build();
    }

    public static Message createMsbRequestMessageWithAckNoPayloadAndTopicTo(String topicTo) {
        Acknowledge simpleAck = new Acknowledge.AcknowledgeBuilder().withResponderId(Utils.generateId()).build();
        return createMsbRequestMessageWithAckNoPayloadAndTopicTo(simpleAck, topicTo, Utils.generateId());
    }

    public static Message createMsbRequestMessageWithAckNoPayloadAndTopicTo(Acknowledge ack, String topicTo, String correlationId) {
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().withCorrelationId(Utils.ifNull(correlationId, Utils.generateId())).setId(Utils.generateId()).withTopics(topic).withMetaBuilder(
                metaBuilder)
                .withPayload(null).withAck(ack).build();
    }

    public static Message createMsbRequestMessageNoPayload(String namespace) {
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(namespace, namespace + ":response:" +
                msbConf.getServiceDetails().getInstanceId());

        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().withCorrelationId(Utils.generateId()).setId(Utils.generateId()).withTopics(topic).withMetaBuilder(metaBuilder)
                .withPayload(null).build();
    }

    public static Message createMsbResponseMessage(String namespace) {
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(namespace, namespace + ":response:" +
                msbConf.getServiceDetails().getInstanceId());
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().withCorrelationId(Utils.generateId()).setId(Utils.generateId()).withTopics(topic)
                .withMetaBuilder(metaBuilder).withPayload(createSimpleResponsePayload()).build();
    }

    public static MessageBuilder createMesageBuilder() {
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics("", "");
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().withCorrelationId(Utils.generateId()).setId(Utils.generateId()).withTopics(topic).withMetaBuilder(metaBuilder);
    }

    public static MsbConfigurations createMsbConfigurations() {
        return new MsbConfigurations(ConfigFactory.load());
    }

    public static Payload createSimpleRequestPayload() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("url", "http://mock/request");
        headers.put("method", "Request");

        Map<String, String> body = new HashMap<String, String>();

        body.put("body", "someRequestBody created at " + Clock.systemDefaultZone().millis());

        return new Payload.PayloadBuilder().withBody(body).withHeaders(headers).build();
    }

    public static Payload createSimpleBroadcastPayload() {
        return createSimpleRequestPayload();
    }

    public static Payload createSimpleResponsePayload() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("statusCode", "200");
        headers.put("method", "Response");

        Map<String, String> body = new HashMap<String, String>();
        body.put("body", "someResponseBody");

        return new Payload.PayloadBuilder().withBody(body).withHeaders(headers).build();
    }

    public static MetaMessageBuilder createSimpleMetaBuilder(MsbConfigurations msbConf, Clock clock) {
        return new MetaMessage.MetaMessageBuilder(null, clock.instant(), msbConf.getServiceDetails(), clock);
    }
    
    public static class TestMsbContextBuilder {
        private Optional<MsbConfigurations> msbConfigOp = Optional.empty();
        private Optional<MessageFactory> messageFactoryOp = Optional.empty(); 
        private Optional<ChannelManager> channelManagerOp = Optional.empty(); 
        private Optional<Clock> clockOp = Optional.empty();
        private Optional<TimeoutManager> timeoutManagerOp = Optional.empty();

        public TestMsbContextBuilder withMsbConfigurations(MsbConfigurations msbConfig) {
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

        public MsbContext build() {
            MsbConfigurations msbConfig = msbConfigOp.orElse(TestUtils.createMsbConfigurations());
            Clock clock = clockOp.orElse(Clock.systemDefaultZone());
            ChannelManager channelManager = channelManagerOp.orElseGet(() -> new ChannelManager(msbConfig, clock, new JsonValidator()));
            MessageFactory messageFactory = messageFactoryOp.orElseGet(() -> new MessageFactory(msbConfig.getServiceDetails(), clock));
            TimeoutManager timeoutManager = timeoutManagerOp.orElseGet(() -> new TimeoutManager(1));
            return new TestMsbContext(msbConfig, messageFactory, channelManager, clock, timeoutManager);
        }

        private static class TestMsbContext extends MsbContext {
            TestMsbContext(MsbConfigurations msbConfig, MessageFactory messageFactory,
                    ChannelManager channelManager, Clock clock, TimeoutManager timeoutManager) {
                super(msbConfig, messageFactory, channelManager, clock, timeoutManager);
            }
        }

    }

}
