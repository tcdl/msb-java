package io.github.tcdl.adapters.mock;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.adapters.AdapterFactoryLoader;
import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.adapters.ProducerAdapter;
import io.github.tcdl.config.MsbConfig;
import org.junit.Test;

/**
 * MockAdapterFactory is an implementation of {@link AdapterFactory}
 * for {@link MockAdapterTest}
 */
public class MockAdapterFactoryTest {

    @Test
    public void testCreateConsumerAdapterReturnMock() {
        MockAdapterFactory mockAdapterFactory = new MockAdapterFactory();
        ConsumerAdapter consumer = mockAdapterFactory.createConsumerAdapter("");
        assertThat(consumer, instanceOf(MockAdapter.class));
    }

    @Test
    public void testCreateProducerAdapterReturnMock() {
        MockAdapterFactory mockAdapterFactory = new MockAdapterFactory();
        ProducerAdapter producer = mockAdapterFactory.createProducerAdapter("");
        assertThat(producer, instanceOf(MockAdapter.class));
    }

}
