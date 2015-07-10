package io.github.tcdl.msb.adapters.mock;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.ProducerAdapter;
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
