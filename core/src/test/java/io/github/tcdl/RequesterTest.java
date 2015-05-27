package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * Created by rdro on 4/27/2015.
 */
public class RequesterTest {

    private MsbContext msbContext;
    private MsbMessageOptions config;

    @Mock
    private MsbMessageOptions messageOptionsMock;


    @Before
    public void setUp() {
        this.config = TestUtils.createSimpleConfig();
        this.msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test(expected = NullPointerException.class)
    public void testRequesterNullConfigsThrowsException() {
        new Requester(null, TestUtils.createMsbRequestMessageNoPayload(), msbContext);
    }

    @Test
    public void testRequesterNotNullConfigsOk() {
        new Requester(config, TestUtils.createMsbRequestMessageNoPayload(), msbContext);
    }
}
