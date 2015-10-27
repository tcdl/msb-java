
package io.github.tcdl.msb.support;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

/**
 * Created by rdro on 4/22/2015.
 */
public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static final String TOPIC_ANNOUNCE = "_channels:announce";
    public static final String TOPIC_HEARTBEAT = "_channels:heartbeat";

    private static final Pattern VALID_TOPIC_REGEXP = Pattern.compile("^_?([a-z0-9\\-]+\\:)+([a-z0-9\\-]+)$");

    public static String generateId() {
        return UUID.randomUUID().toString();
    }

    public static String validateTopic(String topic) {
        if (VALID_TOPIC_REGEXP.matcher(topic).matches()) {
            return topic;
        }
        RuntimeException e = new IllegalArgumentException("\"" + topic + "\" must be an alpha-numeric, colon-delimited string");
        LOG.error("Topic validation error:", e);
        throw e;
    }

    public static <T> T ifNull(T value, T other) {
        return value != null ? value : other;
    }

    /**
     * @throws JsonConversionException if some problems during parsing to JSON
     */
    public static String toJson(Object object, ObjectMapper objectMapper) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to parse to JSON object: [{}] ", object);
            throw new JsonConversionException("Failed parse to JSON", e);
        }
    }

    /**
     * @throws JsonConversionException if problem encountered during parsing JSON
     */
    public static <T> T fromJson(String json, Class<T> clazz, ObjectMapper objectMapper) {
        return fromJson(json,
                new TypeReference<T>() {
                    @Override
                    public Type getType() {
                        return clazz;
                    }
                },
                objectMapper);
    }

    public static <T> T fromJson(String json, TypeReference<T> typeReference, ObjectMapper objectMapper) {
        if (json == null)
            return null;
        try {
            return objectMapper.readValue(json, typeReference);
        } catch (IOException e) {
            LOG.error("Failed to parse from JSON: [{}] to object of type: [{}]", json, typeReference);
            throw new JsonConversionException("Failed parse from JSON", e);
        }
    }

    public static <T> T convert(Object srcObject, Class<T> destClass, ObjectMapper objectMapper) {
        return convert(srcObject,
                new TypeReference<T>() {
                    @Override
                    public Type getType() {
                        return destClass;
                    }
                },
                objectMapper);
    }

    public static <T> T convert(Object srcObject, TypeReference<T> typeReference, ObjectMapper objectMapper) {
        try {
            return objectMapper.convertValue(srcObject, typeReference);
        } catch (Exception e) {
            LOG.error("Failed to convert object [{}] to type: [{}]", srcObject, typeReference.getType());
            throw new JsonConversionException(e.getMessage(), e);
        }
    }

    public static boolean isServiceTopic(String topic) {
        return topic.charAt(0) == '_';
    }

    public static boolean isPayloadPresent(JsonNode rawPayload) {
        return rawPayload != null && !(rawPayload instanceof NullNode);
    }

    /**
     * Shuts down given executor service and waits for all its tasks to complete.
     */
    public static void gracefulShutdown(ExecutorService executorService, String executorServiceName) {
        int pollingTimeout = 10;

        LOG.info(String.format("[thread pool '%s'] Shutting down...", executorServiceName));
        executorService.shutdown();
        try {
            while (!executorService.awaitTermination(pollingTimeout, TimeUnit.SECONDS)) {
                LOG.info(String.format("[thread pool '%s'] Still has some tasks to complete. Waiting...", executorServiceName));
            }
        } catch (InterruptedException e) {
            LOG.warn(String.format("[thread pool '%s'] Interrupted while waiting for termination", executorServiceName), e);
        }
        LOG.info(String.format("[thread pool '%s'] Shut down complete.", executorServiceName));
    }
}
