package io.github.tcdl.msb.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonNodeReader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.github.tcdl.msb.api.exception.JsonSchemaValidationException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Validates JSON against JSON Schema
 */
public class JsonValidator {
    private static final Logger LOG = LoggerFactory.getLogger(JsonValidator.class);
    private static final String ERROR_MESSAGE_TEMPLATE = "Error while validating message using schema '%s'";
    private Map<String, JsonSchema> schemaCache = new ConcurrentHashMap<>();
    private JsonReader jsonReader;

    public JsonValidator() {
        this(new JsonReader());
    }

    public JsonValidator(JsonReader jsonReader) {
        this.jsonReader = jsonReader;
    }

    /**
     * @throws JsonSchemaValidationException if problem encountered during validation.
     */
    public void validate(String json, String schema) {

        Validate.notNull(json, "field 'json' is null");
        Validate.notNull(schema, "field 'schema' is null");

        try {
            JsonNode jsonNode = jsonReader.read(json);

            JsonSchema jsonSchema = schemaCache.computeIfAbsent(schema, s -> {
                try {
                    JsonNode jsonSchemaNode = jsonReader.read(s);
                    JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
                    return factory.getJsonSchema(jsonSchemaNode);
                } catch (Exception e) {
                    throw new JsonSchemaValidationException("Failed reading schema", e);
                }
            });

            ProcessingReport validationReport = jsonSchema.validate(jsonNode);

            if (!validationReport.isSuccess()) {
                throw new JsonSchemaValidationException(validationReport.toString());
            }

        } catch (IOException | ProcessingException e) {
            LOG.error(ERROR_MESSAGE_TEMPLATE, schema);
            LOG.trace("Message: {}", json);
            throw new JsonSchemaValidationException(String.format(ERROR_MESSAGE_TEMPLATE, schema), e);
        }
    }

    public static class JsonReader {

        public JsonNode read(String str) throws IOException {
            return new JsonNodeReader().fromReader(new StringReader(str));
        }
    }
}
