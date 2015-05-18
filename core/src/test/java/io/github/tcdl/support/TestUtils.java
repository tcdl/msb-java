package io.github.tcdl.support;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.springframework.util.ReflectionUtils;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Topics;
import io.github.tcdl.messages.payload.RequestPayload;
import io.github.tcdl.messages.payload.ResponsePayload;

/**
 * Created by rdro on 4/28/2015.
 */
public class TestUtils {

    public static MsbMessageOptions createSimpleConfig() {
        MsbMessageOptions conf = new MsbMessageOptions();
        conf.setNamespace("test:general");
        return conf;
    }

    public static Message createSimpleMsbMessage() {
        Topics topic = new Topics();
        MsbMessageOptions conf = createSimpleConfig();
        MsbConfigurations msbConf = MsbConfigurations.msbConfiguration();
        topic.setTo(conf.getNamespace());
        topic.setResponse(conf.getNamespace() + ":response:" + msbConf.getServiceDetails().getInstanceId());
        return new Message().withCorrelationId(Utils.generateId()).withId(Utils.generateId()).withTopics(topic);
    }

    public static RequestPayload createSimpleRequestPayload() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("url", "http://mock/request");
        headers.put("method", "Request");

        Map<String, String> params = new HashMap<String, String>();
        params.put("query", "someTestParam");

        return new RequestPayload().withHeaders(headers).withParams(params);
    }

    public static ResponsePayload createSimpleResponsePayload() {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("url", "http://mock/response");
        headers.put("method", "Response");
        return new ResponsePayload().withHeaders(headers).withStatusCode(200);
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
