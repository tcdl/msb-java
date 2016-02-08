package io.github.tcdl.msb.api;

import static org.junit.Assert.assertEquals;

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

}