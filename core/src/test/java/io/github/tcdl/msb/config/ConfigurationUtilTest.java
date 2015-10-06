package io.github.tcdl.msb.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.github.tcdl.msb.api.exception.ConfigurationException;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConfigurationUtilTest {

    @Test
    public void testGetMandatoryBooleanExists() {
        String paramName = "parameter.boolean";
        Config config = createConfigWithValue(paramName, "true");

        boolean value = ConfigurationUtil.getBoolean(config, paramName);

        assertTrue(value);
    }

    @Test (expected = ConfigurationException.class)
    public void testGetMandatoryBooleanNotExists() {
        String param = "parameter.boolean";
        Config config = ConfigFactory.empty();

        ConfigurationUtil.getBoolean(config, param);
    }

    @Test
    public void testGetOptionalBooleanExists() {
        String paramName = "parameter.boolean";
        Config config = createConfigWithValue(paramName, "true");

        Optional<Boolean> value = ConfigurationUtil.getOptionalBoolean(config, paramName);

        assertTrue(value.isPresent());
        assertTrue(value.get());
    }

    @Test
    public void testGetOptionalBooleanNotExists() {
        String paramName = "parameter.boolean";
        Config config = ConfigFactory.empty();

        Optional<Boolean> value = ConfigurationUtil.getOptionalBoolean(config, paramName);

        assertFalse(value.isPresent());
    }

    @Test
    public void testGetMandatoryStringExists() {
        String paramName = "parameter.string";
        String expectedValue = "some value";
        Config config = createConfigWithValue(paramName, expectedValue);

        String value = ConfigurationUtil.getString(config, paramName);
        assertEquals(expectedValue, value);
    }

    @Test(expected = ConfigurationException.class)
    public void testGetMandatoryStringNotExists() {
        String param = "parameter.string";
        Config config = ConfigFactory.empty();

        ConfigurationUtil.getString(config, param);
    }

    @Test
    public void testGetOptionalStringExists() {
        String paramName = "parameter.string";
        String expectedValue = "some value";
        Config config = createConfigWithValue(paramName, expectedValue);

        Optional<String> value = ConfigurationUtil.getOptionalString(config, paramName);

        assertTrue(value.isPresent());
        assertEquals(expectedValue, value.get());
    }

    @Test
    public void testGetOptionalStringNotExists() {
        String paramName = "parameter.string";
        Config config = ConfigFactory.empty();

        Optional<String> value = ConfigurationUtil.getOptionalString(config, paramName);

        assertFalse(value.isPresent());
    }

    @Test
    public void testGetMandatoryIntegerExists() {
        String paramName = "parameter.integer";
        int expectedValue = 1000;
        Config config = createConfigWithValue(paramName, expectedValue);

        int value = ConfigurationUtil.getInt(config, paramName);

        assertEquals(expectedValue, value);
    }

    @Test(expected = ConfigurationException.class)
    public void testGetMandatoryIntegerNotExists() {
        String param = "parameter.integer";
        Config config = ConfigFactory.empty();
        ConfigurationUtil.getInt(config, param);
    }

    @Test
    public void testGetMandatoryLongExists() {
        String paramName = "parameter.long";
        long expectedValue = 1000L;
        Config config = createConfigWithValue(paramName, expectedValue);

        long value = ConfigurationUtil.getLong(config, paramName);

        assertEquals(expectedValue, value);
    }

    @Test(expected = ConfigurationException.class)
    public void testGetMandatoryLongNotExists() {
        String paramName = "parameter.long";
        Config config = ConfigFactory.empty();
        ConfigurationUtil.getLong(config, paramName);
    }

    private Config createConfigWithValue(String paramName, Object paramValue) {
        return ConfigFactory.empty()
                .withValue(paramName, ConfigValueFactory.fromAnyRef(paramValue));
    }
}
