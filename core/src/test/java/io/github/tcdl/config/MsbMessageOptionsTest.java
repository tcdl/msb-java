package io.github.tcdl.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;

import io.github.tcdl.Collector;
import org.junit.Test;

/**
 * Created by ruslan on 15.06.15.
 */
public class MsbMessageOptionsTest {

    @Test
    public void testGetWaitForResponsesConfigsNull() {
        MsbMessageOptions msbMessageOptions = new MsbMessageOptions();
        msbMessageOptions.setWaitForResponses(null);

        assertEquals("expect 0 if MessageOptions.waitForResponses is null", 0, msbMessageOptions.getWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsMinusOne() {
        MsbMessageOptions msbMessageOptions = new MsbMessageOptions();
        msbMessageOptions.setWaitForResponses(-1);

        assertEquals("expect 0 if MessageOptions.waitForResponses is -1", 0, msbMessageOptions.getWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsPositive() {
        MsbMessageOptions msbMessageOptions = new MsbMessageOptions();
        msbMessageOptions.setWaitForResponses(100);

        assertEquals("expect 100 if MessageOptions.waitForResponses is 100", 100, msbMessageOptions.getWaitForResponses());
    }

}
