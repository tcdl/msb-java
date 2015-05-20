package io.github.tcdl.support;

import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.exception.JsonSchemaValidationException;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jackson.JsonNodeReader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

/**
 * Created by rdro on 4/22/2015.
 */
public class Utils {

    public static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private final static Pattern VALID_TOPIC_REGEXP = Pattern.compile("^_?([a-z0-9\\-]+\\:)+([a-z0-9\\-]+)$");
    private final static SimpleDateFormat JSON_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    public static String generateId() {
        return UUID.randomUUID().toString();// TODO
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
        ObjectMapper objectMapper =  new ObjectMapper().setDateFormat(JSON_DATE_FORMAT);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return objectMapper;
    }

    public static String toJson(Object object) throws JsonConversionException {
        ObjectMapper mapper = getMsbJsonObjectMapper();
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            LOG.error("Failed parse to JSON from: " + object, e);
            throw new JsonConversionException(e.getMessage());
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) throws JsonConversionException {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            LOG.error("Failed parse JSON: {} to object of type: {}", json, clazz, e);
            throw new JsonConversionException(e.getMessage());
        }
    }

    public static void validateJsonWithSchema(String json, String schema) throws JsonSchemaValidationException {
        Validate.notNull(json, "field 'json' is null");
        Validate.notNull(schema, "field 'schema' is null");
        try {
            JsonNode jsonNode = new JsonNodeReader().fromInputStream(IOUtils.toInputStream(json));
            JsonNode jsonSchemaNode = new JsonNodeReader().fromInputStream(IOUtils.toInputStream(schema));

            JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
            JsonSchema jsonSchema = factory.getJsonSchema(jsonSchemaNode);

            ProcessingReport validationReport = jsonSchema.validate(jsonNode);

            if (!validationReport.isSuccess()) {
                throw new JsonSchemaValidationException(validationReport.toString());
            }

        } catch (IOException | ProcessingException e) {
            throw new JsonSchemaValidationException(e.getMessage());
        }
    }
}
