package io.github.tcdl.msb.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.api.exception.JsonSchemaValidationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JsonValidatorTest {

    private String schema = "{\"properties\":{\"id\":{\"type\":\"string\"}}, \"required\": [\"id\"]}";
    private JsonValidator validator;

    private JsonValidator.JsonReader jsonReaderMock;
    private ObjectMapper mapper;

    @Before
    public void setUp() throws Exception {
        mapper = TestUtils.createMessageMapper();
        jsonReaderMock = spy(JsonValidator.JsonReader.class);
        validator = new JsonValidator(jsonReaderMock);
    }

    @After
    public void tearDown() {
        reset(jsonReaderMock);
    }

    @Test
    public void testValidateSuccess() throws Exception {
        String namespace = TestUtils.getSimpleNamespace();
        String jsonMessage = Utils.toJson(TestUtils.createMsbRequestMessageNoPayload(namespace), mapper);

        try {
            validator.validate(jsonMessage, schema);
        } catch(Exception e){
            fail("Validation should not fail");
        }
    }

    @Test
    public void testCachingJsonSchema() throws Exception {
        String namespace = TestUtils.getSimpleNamespace();
        String jsonMessage = Utils.toJson(TestUtils.createMsbRequestMessageNoPayload(namespace), mapper);

        // validate first time. 2 calls for message and schema
        validator.validate(jsonMessage, schema);
        verify(jsonReaderMock, times(2)).read(any());

        // validate one more time. expecting 1 call
        reset(jsonReaderMock);
        validator.validate(jsonMessage, schema);
        verify(jsonReaderMock, only()).read(any());
    }

    @Test(expected = NullPointerException.class)
    public void testValidateNullJsonFail() throws Exception {
        validator.validate(null, schema);
    }

    @Test(expected = NullPointerException.class)
    public void testValidateNullJsonSchemaFail() throws Exception {
        String namespace = TestUtils.getSimpleNamespace();
        String jsonMessage = Utils.toJson(TestUtils.createMsbRequestMessageNoPayload(namespace), mapper);
        validator.validate(jsonMessage, null);
    }

    @Test(expected = JsonSchemaValidationException.class)
    public void testValidateInvalidJsonFail() throws Exception {
        String invalidJson = "invalid message";
        validator.validate(invalidJson, schema);
    }

    @Test(expected = JsonSchemaValidationException.class)
    public void testValidateValidJsonNotMatchingSchemaFail() throws Exception {
        String invalidJson = "{\"param\":\"value\"}";
        validator.validate(invalidJson, schema);
    }
}
