package io.github.tcdl.support;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.tcdl.MsbContext;
import io.github.tcdl.exception.JsonSchemaValidationException;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Created by rdrozdov-tc on 6/11/15.
 */
public class JsonValidatorTest {

    private String messageSchema =  new MsbContext.MsbContextBuilder().build().getMsbConfig().getSchema();
    private JsonValidator validator;

    @Before
    public void setUp() {
        validator = new JsonValidator();
    }

    @Test
    public void testValidateSuccess() throws Exception {
        String jsonMessage = Utils.toJson(TestUtils.createMsbRequestMessageNoPayload());

        try {
            validator.validate(jsonMessage, messageSchema);
        } catch(Exception e){
            fail("Validation should not fail");
        }
    }

    @Test
    public void testCachingJsonSchema() throws Exception {
        String jsonMessage = Utils.toJson(TestUtils.createMsbRequestMessageNoPayload());

        Map<String, JsonNode> jsonSchemaCacheSpy = spy(ConcurrentHashMap.class);
        validator.JSON_SCHEMA_CACHE = jsonSchemaCacheSpy;

        // validate first time
        validator.validate(jsonMessage, messageSchema);
        verify(jsonSchemaCacheSpy).putIfAbsent(eq(messageSchema), any(JsonNode.class));

        // validate one more time
        reset(jsonSchemaCacheSpy);
        validator.validate(jsonMessage, messageSchema);
        verify(jsonSchemaCacheSpy, never()).putIfAbsent(eq(messageSchema), any(JsonNode.class));
    }

    @Test(expected = NullPointerException.class)
    public void testValidateNullJsonFail() throws Exception {
        validator.validate(null, messageSchema);
    }

    @Test(expected = NullPointerException.class)
    public void testValidateNullJsonSchemaFail() throws Exception {
        String jsonMessage = Utils.toJson(TestUtils.createMsbRequestMessageNoPayload());
        validator.validate(jsonMessage, null);
    }

    @Test(expected = JsonSchemaValidationException.class)
    public void testValidateInvalidJsonFail() throws Exception {
        String invalidJson = "invalid message";
        validator.validate(invalidJson, messageSchema);
    }

    @Test(expected = JsonSchemaValidationException.class)
    public void testValidateValidJsonNotMatchingSchemaFail() throws Exception {
        String invalidJson = "{\"param\":\"value\"}";
        validator.validate(invalidJson, messageSchema);
    }
}
