package io.github.tcdl.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonNodeReader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.github.tcdl.exception.JsonSchemaValidationException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Validates json against json schema
 *
 * Created by rdrozdov-tc on 6/11/15.
 */
public class JsonValidator {

    private Map<String, JsonNode> schemaCache = new ConcurrentHashMap<>();
    private JsonReader jsonReader;

    public JsonValidator() {
        this(new JsonReader());
    }

    public JsonValidator(JsonReader jsonReader) {
        this.jsonReader = jsonReader;
    }

    public void validate(String json, String schema) throws JsonSchemaValidationException {

        Validate.notNull(json, "field 'json' is null");
        Validate.notNull(schema, "field 'schema' is null");

        try {
            JsonNode jsonNode = jsonReader.read(IOUtils.toInputStream(json));
            JsonNode jsonSchemaNode;

            if (!schemaCache.containsKey(schema)) {
                jsonSchemaNode = jsonReader.read(IOUtils.toInputStream(schema));
                schemaCache.putIfAbsent(schema, jsonSchemaNode);
            }
            jsonSchemaNode = schemaCache.get(schema);

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

    public static class JsonReader {

        public JsonNode read(InputStream inputStream) throws IOException{
            return new JsonNodeReader().fromInputStream(inputStream);
        }
    }
}
