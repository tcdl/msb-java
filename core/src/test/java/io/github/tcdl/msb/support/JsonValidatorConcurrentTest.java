package io.github.tcdl.msb.support;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.junittoolbox.MultithreadingTester;
import org.junit.Before;
import org.junit.Test;

public class JsonValidatorConcurrentTest {

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

    @Test
    public void testCachingJsonSchemaConcurrentInteraction() throws Exception {
        String namespace = TestUtils.getSimpleNamespace();
        String jsonMessage = Utils.toJson(TestUtils.createMsbRequestMessageNoPayload(namespace), mapper);

        int numberOfThreads = 10;
        int numberOfInvocationsPerThread = 20;

        new MultithreadingTester().numThreads(numberOfThreads).numRoundsPerThread(numberOfInvocationsPerThread).add(() -> {
            validator.validate(jsonMessage, schema);
        }).run();

        // first time we validate schema is read and cashed
        verify(jsonReaderMock, times(numberOfThreads * numberOfInvocationsPerThread + 1)).read(anyString());
    }
}
