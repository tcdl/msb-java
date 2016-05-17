package io.github.tcdl.msb.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.Test;

public class RequestOptionsTest {

    @Test
    public void testGetWaitForResponsesConfigsNull() {
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withWaitForResponses(null)
                .build();

        assertEquals( RequestOptions.WAIT_FOR_RESPONSES_UNTIL_TIMEOUT, requestOptions.getWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsMinusOne() {
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withWaitForResponses(-1)
                .build();

        assertEquals(RequestOptions.WAIT_FOR_RESPONSES_UNTIL_TIMEOUT, requestOptions.getWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsPositive() {
        int responsesRemaining = 100;
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withWaitForResponses(responsesRemaining)
                .build();

        assertEquals(responsesRemaining, requestOptions.getWaitForResponses());
    }

    @Test
    public void testForwardNamespace() {
        String forwardNamespace = "test:forward";
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withForwardNamespace(forwardNamespace)
                .build();

        assertEquals(forwardNamespace, requestOptions.getForwardNamespace());
    }

    @Test
    public void testBuilderFromExistingRequestOptions() throws Exception {

        MessageTemplate sourceMessageTemplate = new MessageTemplate();

        Integer ackTimeout = 1;
        Integer responseTimeout = 2;
        String forwardNamespace = "forward:namespace";
        int waitForResponses = 3;

        RequestOptions source = new RequestOptions.Builder()
                .withAckTimeout(ackTimeout)
                .withResponseTimeout(responseTimeout)
                .withForwardNamespace(forwardNamespace)
                .withWaitForResponses(waitForResponses)
                .withMessageTemplate(sourceMessageTemplate)
                .build();

        RequestOptions.Builder builder = new RequestOptions.Builder().from(source);
        RequestOptions result = builder.build();

        assertEquals(ackTimeout, result.getAckTimeout());
        assertEquals(responseTimeout, result.getResponseTimeout());
        assertEquals(waitForResponses, result.getWaitForResponses());
        assertEquals(forwardNamespace, result.getForwardNamespace());
        assertSame(sourceMessageTemplate, result.getMessageTemplate());
    }
}