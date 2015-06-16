package io.github.tcdl.config;

import com.typesafe.config.Config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.github.tcdl.exception.ConfigurationException;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;

/**
 * Created by rdrozdov-tc on 6/11/15.
 */
@RunWith(MockitoJUnitRunner.class)
public class ConfigurationUtilTest {

    @Mock
    private Config config;

    @Test
    public void testGetBooleanExists() {
        String param = "parameter.boolean";
        when(config.hasPath(param)).thenReturn(true);

        ConfigurationUtil.getBoolean(config, param, true);
        verify(config).getBoolean(eq(param));
    }

    @Test
    public void testGetBooleanNotExists() {
        String param = "parameter.boolean";
        Boolean fallback = true;

        when(config.hasPath(param)).thenReturn(false);

        Boolean value = ConfigurationUtil.getBoolean(config, param, fallback);

        verify(config, never()).getBoolean(eq(param));
        assertEquals(fallback, value);
    }

    @Test
    public void testGetMandatoryBooleanExists() {
        String param = "parameter.boolean";
        when(config.hasPath(param)).thenReturn(true);

        ConfigurationUtil.getBoolean(config, param);
        verify(config).getBoolean(eq(param));
    }

    @Test (expected = ConfigurationException.class)
    public void testGetMandatoryBooleanNotExists() {
        String param = "parameter.boolean";
        when(config.hasPath(param)).thenReturn(false);

        ConfigurationUtil.getBoolean(config, param);
    }

    @Test
    public void testGetOptionalBooleanExists() {
        String param = "parameter.boolean";

        when(config.hasPath(param)).thenReturn(true);

        Optional<Boolean> value = ConfigurationUtil.getOptionalBoolean(config, param);

        verify(config).getBoolean(eq(param));
        assertTrue(value.isPresent());
    }

    @Test
    public void testGetOptionalBooleanNotExists() {
        String param = "parameter.boolean";

        when(config.hasPath(param)).thenReturn(false);

        Optional<Boolean> value = ConfigurationUtil.getOptionalBoolean(config, param);

        verify(config, never()).getBoolean(eq(param));
        assertFalse(value.isPresent());
    }

    @Test
    public void testGetStringExists() {
        String param = "parameter.string";
        String fallback = "if-empty";

        when(config.hasPath(param)).thenReturn(true);

        ConfigurationUtil.getString(config, param, fallback);
        verify(config).getString(eq(param));
    }

    @Test
    public void testGetStringNotExists() {
        String param = "parameter.string";
        String fallback = "if-empty";

        when(config.hasPath(param)).thenReturn(false);

        String value = ConfigurationUtil.getString(config, param, fallback);

        verify(config, never()).getString(eq(param));
        assertEquals(fallback, value);
    }

    @Test
    public void testGetMandatoryStringExists() {
        String param = "parameter.string";

        when(config.hasPath(param)).thenReturn(true);

        ConfigurationUtil.getString(config, param);
        verify(config).getString(eq(param));
    }

    @Test (expected = ConfigurationException.class)
    public void testGetMandatoryStringNotExists() {
        String param = "parameter.string";

        when(config.hasPath(param)).thenReturn(false);

        ConfigurationUtil.getString(config, param);
    }

    @Test
    public void testGetOptionalStringExists() {
        String param = "parameter.string";

        when(config.hasPath(param)).thenReturn(true);

        ConfigurationUtil.getOptionalString(config, param);

        verify(config).getString(eq(param));
    }

    @Test
    public void testGetOptionalStringNotExists() {
        String param = "parameter.string";

        when(config.hasPath(param)).thenReturn(false);

        Optional<String> value = ConfigurationUtil.getOptionalString(config, param);

        verify(config, never()).getString(eq(param));
        assertFalse(value.isPresent());
    }

    @Test
    public void testGetIntegerExists() {
        String param = "parameter.integer";
        Integer fallback = 0;

        when(config.hasPath(param)).thenReturn(true);

        ConfigurationUtil.getInt(config, param, fallback);

        verify(config).getInt(eq(param));
    }

    @Test
    public void testGetIntegerNotExists() {
        String param = "parameter.integer";
        Integer fallback = 0;

        when(config.hasPath(param)).thenReturn(true);

        Integer value = ConfigurationUtil.getInt(config, param, fallback);

        verify(config).getInt(eq(param));
        assertEquals(fallback, value);
    }
    
    @Test
    public void testGetMandatoryIntegerExists() {
        String param = "parameter.integer";

        when(config.hasPath(param)).thenReturn(true);

        ConfigurationUtil.getInt(config, param);

        verify(config).getInt(eq(param));
    }

    @Test (expected = ConfigurationException.class)
    public void testGetMandatoryIntegerNotExists() {
        String param = "parameter.integer";

        when(config.hasPath(param)).thenReturn(false);

        ConfigurationUtil.getInt(config, param);
    }


}
