package io.github.tcdl.support;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import io.github.tcdl.api.exception.JsonConversionException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

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
        throw new IllegalArgumentException("\"" + topic + "\" must be an alpha-numeric, colon-delimited string");
    }

    public static Integer getPid() {
        return Integer.valueOf(StringUtils.substringBefore(ManagementFactory.getRuntimeMXBean().getName(), "@"));
    }

    public static <T> T ifNull(T value, T other) {
        return value != null ? value : other;
    }

    public static ObjectMapper getMsbJsonObjectMapper() {
        return new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .registerModule(new JSR310Module());
    }

    /**
     * @throws JsonConversionException if some problems during parsing to JSON
     */
    public static String toJson(Object object) {
        ObjectMapper mapper = getMsbJsonObjectMapper();
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            LOG.error("Failed parse to JSON from: " + object, e);
            throw new JsonConversionException(e.getMessage());
        }
    }

    /**
     * @throws JsonConversionException if some problems during parsing JSON
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        if (json == null) return null;
        ObjectMapper mapper = getMsbJsonObjectMapper();
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            LOG.error("Failed parse JSON: {} to object of type: {}", json, clazz, e);
            throw new JsonConversionException(e.getMessage());
        }
    }

    public static boolean isServiceTopic(String topic) {
        return topic.charAt(0) == '_';
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
