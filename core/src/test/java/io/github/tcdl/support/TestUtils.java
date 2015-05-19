package io.github.tcdl.support;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.util.ReflectionUtils;

import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.MetaMessage;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.Utils;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Topics;

/**
 * Created by rdro on 4/28/2015.
 */
public class TestUtils {

    public static MsbMessageOptions createSimpleConfig() {
        MsbMessageOptions conf = new MsbMessageOptions();
        conf.setNamespace("test:general");
        return conf;
    }

    public static Message createMsbRequestMessageWithPayload() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = MsbConfigurations.msbConfiguration();

        Topics topic = new Topics.TopicsBuilder().setTo(conf.getNamespace())
                .setResponse(conf.getNamespace() + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessage meta = createSimpleMeta(msbConf);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMeta(meta)
                .setPayload(createSimpleRequestPayload()).build();
    }
    
    public static Message createMsbRequestMessageNoPayload() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = MsbConfigurations.msbConfiguration();

        Topics topic = new Topics.TopicsBuilder().setTo(conf.getNamespace())
                .setResponse(conf.getNamespace() + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();

        MetaMessage meta = createSimpleMeta(msbConf);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMeta(meta)
                .setPayload(null).build();
    }
    
    public static Message createMsbResponseMessage() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = MsbConfigurations.msbConfiguration();

        Topics topic = new Topics.TopicsBuilder().setResponse(conf.getNamespace())
                .setTo(conf.getNamespace() + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessage meta = createSimpleMeta(msbConf);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic)
                .setMeta(meta).setPayload(createSimpleResponsePayload()).build();
    }

    public static Message createMsbAckMessage() {
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = MsbConfigurations.msbConfiguration();

        Topics topic = new Topics.TopicsBuilder().setResponse(conf.getNamespace())
                .setTo(conf.getNamespace() + ":response:" + msbConf.getServiceDetails().getInstanceId()).build();
        MetaMessage meta = createSimpleMeta(msbConf);
        return new Message.MessageBuilder().setCorrelationId(Utils.generateId()).setId(Utils.generateId()).setTopics(topic).setMeta(meta)
                .setAck(new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).build()).build();
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
    
    public static MetaMessage createSimpleMeta(MsbConfigurations msbConf) {
        return new MetaMessage.MetaMessageBuilder(0, new Date(), msbConf.getServiceDetails()).computeDurationMs().build();
    }

    public static <T> T _g(Object object, String fieldName) {
        Field field = ReflectionUtils.findField(object.getClass(), fieldName);
        ReflectionUtils.makeAccessible(field);
        return (T) ReflectionUtils.getField(field, object);
    }

    public static void _s(Object object, String fieldName, Object value) {
        Field field = ReflectionUtils.findField(object.getClass(), fieldName);
        ReflectionUtils.makeAccessible(field);
        ReflectionUtils.setField(field, object, value);
    }

    public static <T> T _m(Object object, String methodName) {
        Method method = ReflectionUtils.findMethod(object.getClass(), methodName);
        ReflectionUtils.makeAccessible(method);
        return (T) ReflectionUtils.invokeMethod(method, object, null);
    }

    public static <T> T _m(Object object, String methodName, Object[] args, Class... argTypes) {
        Method method = ReflectionUtils.findMethod(object.getClass(), methodName, argTypes);
        ReflectionUtils.makeAccessible(method);
        return (T) ReflectionUtils.invokeMethod(method, object, args);
    }
}