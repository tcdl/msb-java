package io.github.tcdl.support;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.ChannelManager;
import io.github.tcdl.MsbContext;
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
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Method;
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
        ChannelManager channelManager = new ChannelManager(msbConfig, clock);
        MessageFactory messageFactory = new MessageFactory(msbConfig.getServiceDetails(), clock);

        return new MsbContext(msbConfig, messageFactory, channelManager, clock);
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

        Topics topic = new Topics.TopicsBuilder().setTo(topicTo)
                .setResponse(topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMetaBuilder(metaBuilder)
                .setPayload(createSimpleRequestPayload()).build();
    }

    public static Message createMsbRequestMessageWithAckNoPayloadAndTopicTo(String topicTo) {
        Acknowledge simpleAck = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).build();
        return createMsbRequestMessageWithAckNoPayloadAndTopicTo(simpleAck, topicTo);
    }

    public static Message createMsbRequestMessageWithAckNoPayloadAndTopicTo(Acknowledge ack, String topicTo) {
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics.TopicsBuilder().setTo(topicTo)
                .setResponse(topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMetaBuilder(metaBuilder)
                .setPayload(null).setAck(ack).build();
    }

    public static Message createMsbRequestMessageNoPayload() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics.TopicsBuilder().setTo(conf.getNamespace())
                .setResponse(conf.getNamespace() + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();

        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMetaBuilder(metaBuilder)
                .setPayload(null).build();
    }

    public static Message createMsbResponseMessage() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics.TopicsBuilder().setResponse(conf.getNamespace())
                .setTo(conf.getNamespace() + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessageBuilder metaBuilder = createSimpleMetaBuilder(msbConf, clock);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic)
                .setMetaBuilder(metaBuilder).setPayload(createSimpleResponsePayload()).build();
    }

    public static MessageBuilder createMesageBuilder() {
        MsbConfigurations msbConf = createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();

        Topics topic = new Topics.TopicsBuilder().setResponse("")
                .setTo("").build();
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

    public static Payload createSimpleResponsePayloadWithBodyAndStatusCode(String respBody, int respStatusCode) {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("statusCode", String.valueOf(respStatusCode));
        headers.put("method", "Response");

        Map<String, String> body = new HashMap<String, String>();
        body.put("body", respBody);

        return new Payload.PayloadBuilder().setBody(body).setHeaders(headers).build();
    }

    public static MetaMessageBuilder createSimpleMetaBuilder(MsbConfigurations msbConf, Clock clock) {
        return new MetaMessage.MetaMessageBuilder(null, clock.instant(), msbConf.getServiceDetails(), clock);
    }

    public static <T> T _g(Object object, String fieldName) {
        try {
            return (T) FieldUtils.readField(object, fieldName, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void _s(Object object, String fieldName, Object value) {
        try {
            FieldUtils.writeField(object, fieldName, value, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T _m(Object object, String methodName) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName);
            method.setAccessible(true);
            return (T) method.invoke(object);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T _m(Object object, String methodName, Object[] args, Class... argTypes) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
            method.setAccessible(true);
            return (T) method.invoke(object, args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
