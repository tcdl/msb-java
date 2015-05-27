package io.github.tcdl.support;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MetaMessage;
import io.github.tcdl.messages.Topics;
import io.github.tcdl.messages.payload.Payload;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.reflect.FieldUtils;

import com.typesafe.config.ConfigFactory;

/**
 * Created by rdro on 4/28/2015.
 */
public class TestUtils {

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

        Topics topic = new Topics.TopicsBuilder().setTo(topicTo)
                .setResponse(topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessage meta = createSimpleMeta(msbConf);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMeta(meta)
                .setPayload(createSimpleRequestPayload()).build();
    }

    public static Message createMsbRequestMessageWithAckNoPayloadAndTopicTo(String topicTo) {
        Acknowledge simpleAck = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).build();
        return createMsbRequestMessageWithAckNoPayloadAndTopicTo(simpleAck, topicTo);
    }

    public static Message createMsbRequestMessageWithAckNoPayloadAndTopicTo(Acknowledge ack, String topicTo) {
        MsbConfigurations msbConf = createMsbConfigurations();

        Topics topic = new Topics.TopicsBuilder().setTo(topicTo)
                .setResponse(topicTo + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessage meta = createSimpleMeta(msbConf);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMeta(meta)
                .setPayload(null).setAck(ack).build();
    }

    public static Message createMsbRequestMessageNoPayload() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = createMsbConfigurations();

        Topics topic = new Topics.TopicsBuilder().setTo(conf.getNamespace())
                .setResponse(conf.getNamespace() + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();

        MetaMessage meta = createSimpleMeta(msbConf);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMeta(meta)
                .setPayload(null).build();
    }

    public static Message createMsbResponseMessage() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = createMsbConfigurations();

        Topics topic = new Topics.TopicsBuilder().setResponse(conf.getNamespace())
                .setTo(conf.getNamespace() + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessage meta = createSimpleMeta(msbConf);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic)
                .setMeta(meta).setPayload(createSimpleResponsePayload()).build();
    }

    public static Message createMsbAckMessage() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = createMsbConfigurations();

        Topics topic = new Topics.TopicsBuilder().setResponse(conf.getNamespace())
                .setTo(conf.getNamespace() + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessage meta = createSimpleMeta(msbConf);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMeta(meta)
                .setAck(new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).build()).build();
    }

    public static MsbConfigurations createMsbConfigurations() {
        return new MsbConfigurations(ConfigFactory.load());
    }

    public static Payload createSimpleRequestPayload() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("url", "http://mock/request");
        headers.put("method", "Request");

        Map<String, String> body = new HashMap<String, String>();
        body.put("body", "someRequestBody");

        return new Payload.PayloadBuilder().setBody(body).setHeaders(headers).build();
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

    public static MetaMessage createSimpleMeta(MsbConfigurations msbConf) {
        return new MetaMessage.MetaMessageBuilder(0, new Date(), msbConf.getServiceDetails()).computeDurationMs().build();
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
