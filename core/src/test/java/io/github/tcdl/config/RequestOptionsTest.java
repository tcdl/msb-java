package io.github.tcdl.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by ruslan on 15.06.15.
 */
public class RequestOptionsTest {

    @Test
    public void testGetWaitForResponsesConfigsNull() {
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setWaitForResponses(null);

        assertEquals("expect 0 if MessageOptions.waitForResponses is null", Integer.valueOf(0), requestOptions.getWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsMinusOne() {
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setWaitForResponses(-1);

        assertEquals("expect 0 if MessageOptions.waitForResponses is -1", Integer.valueOf(0), requestOptions.getWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsPositive() {
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setWaitForResponses(100);

        assertEquals("expect 100 if MessageOptions.waitForResponses is 100", Integer.valueOf(100), requestOptions.getWaitForResponses());
    }

}
