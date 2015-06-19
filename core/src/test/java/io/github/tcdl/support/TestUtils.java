package io.github.tcdl.support;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.ChannelManager;
import io.github.tcdl.MsbContext;
import io.github.tcdl.TimeoutManager;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.MetaMessage;
import io.github.tcdl.messages.MetaMessage.MetaMessageBuilder;
import io.github.tcdl.messages.Topics;
import io.github.tcdl.messages.payload.Payload;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rdro on 4/28/2015.
 */
public class TestUtils {

    public static MsbContext createSimpleMsbContext() {
        MsbConfigurations msbConfig = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();
        JsonValidator validator = new JsonValidator();
        ChannelManager channelManager = new ChannelManager(msbConfig, clock, validator);
        MessageFactory messageFactory = new MessageFactory(msbConfig.getServiceDetails(), clock);
        TimeoutManager timeoutManager = new TimeoutManager(1);

        return new MsbContext(msbConfig, messageFactory, channelManager, clock, timeoutManager);
    }

    public static MsbMessageOptions createSimpleConfig() {
        MsbMessageOptions conf = new MsbMessageOptions();
        conf.setNamespace("test:general");
        return conf;
    }

    public static MsbMessageOptions createSimpleConfigSetNamespace(String namespace) {
        MsbMessageOptions conf = new MsbMessageOptions();
        conf.setNamespace(namespace);
        return conf;
    }

    public static Message createMsbRequestMessageWithPayloadAndTopicTo(String topicTo) {
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMetaBuilder(metaBuilder)
                .setPayload(createSimpleRequestPayload()).build();
    }

    public static Message createMsbRequestMessageWithAckNoPayloadAndTopicTo(String topicTo) {
        Acknowledge simpleAck = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).build();
        return createMsbRequestMessageWithAckNoPayloadAndTopicTo(simpleAck, topicTo, Utils.generateId());
    }

    public static Message createMsbRequestMessageWithAckNoPayloadAndTopicTo(Acknowledge ack, String topicTo, String correlationId) {
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(topicTo, topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId());
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().setCorrelationId(Utils.ifNull(correlationId, Utils.generateId())).setId(Utils.generateId()).setTopics(topic).setMetaBuilder(metaBuilder)
                .setPayload(null).setAck(ack).build();
    }

    public static Message createMsbRequestMessageNoPayload() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(conf.getNamespace(), conf.getNamespace() + ":response:" +
                msbConf.getServiceDetails().getInstanceId());

        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMetaBuilder(metaBuilder)
                .setPayload(null).build();
    }

    public static Message createMsbResponseMessage() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics(conf.getNamespace(), conf.getNamespace() + ":response:" +
                msbConf.getServiceDetails().getInstanceId());
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic)
                .setMetaBuilder(metaBuilder).setPayload(createSimpleResponsePayload()).build();
    }

    public static MessageBuilder createMesageBuilder() {
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics("", "");
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMetaBuilder(metaBuilder);
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

        return new Payload.PayloadBuilder().setBody(body).setHeaders(headers).build();
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

        return new Payload.PayloadBuilder().setBody(body).setHeaders(headers).build();
    }

    public static MetaMessageBuilder createSimpleMetaBuilder(MsbConfigurations msbConf, Clock clock) {
        return new MetaMessage.MetaMessageBuilder(null, clock.instant(), msbConf.getServiceDetails(), clock);
    }

}
