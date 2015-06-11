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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Validates json against json schema
 *
 * Created by rdrozdov-tc on 6/11/15.
 */
public class JsonValidator {

    Map<String, JsonNode> JSON_SCHEMA_CACHE = new ConcurrentHashMap<>();

    public void validate(String json, String schema) throws JsonSchemaValidationException {

        Validate.notNull(json, "field 'json' is null");
        Validate.notNull(schema, "field 'schema' is null");

        try {
            JsonNode jsonNode = new JsonNodeReader().fromInputStream(IOUtils.toInputStream(json));
            JsonNode jsonSchemaNode;

            if (!JSON_SCHEMA_CACHE.containsKey(schema)) {
                jsonSchemaNode = new JsonNodeReader().fromInputStream(IOUtils.toInputStream(schema));
                JSON_SCHEMA_CACHE.putIfAbsent(schema, jsonSchemaNode);
            }
            jsonSchemaNode = JSON_SCHEMA_CACHE.get(schema);

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
