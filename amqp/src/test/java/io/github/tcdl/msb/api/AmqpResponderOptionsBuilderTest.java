package io.github.tcdl.msb.api;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class AmqpResponderOptionsBuilderTest {

    @Test
    public void build_shouldSetHashBindingKeyByDefault() throws Exception {
        ResponderOptions responderOptions = new AmqpResponderOptions.Builder().build();
        assertEquals(Collections.singleton("#"), responderOptions.getBindingKeys());
    }
}