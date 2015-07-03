package io.github.tcdl.api;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Created by ruslan on 15.06.15.
 */
public class RequestOptionsTest {

    @Test
    public void testGetWaitForResponsesConfigsNull() {
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withWaitForResponses(null)
                .build();

        assertEquals("expect 0 if MessageOptions.waitForResponses is null", Integer.valueOf(0), requestOptions.getWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsMinusOne() {
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withWaitForResponses(-1)
                .build();

        assertEquals("expect 0 if MessageOptions.waitForResponses is -1", Integer.valueOf(0), requestOptions.getWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsPositive() {
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withWaitForResponses(100)
                .build();

        assertEquals("expect 100 if MessageOptions.waitForResponses is 100", Integer.valueOf(100), requestOptions.getWaitForResponses());
    }

}
