package io.github.tcdl.msb.adapters.mock;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
    public void testCreateConsumerAdapter() {
        MockAdapterFactory mockAdapterFactory = new MockAdapterFactory();
        ConsumerAdapter consumer = mockAdapterFactory.createConsumerAdapter("", true);
        assertThat(consumer, instanceOf(MockAdapter.class));
        assertTrue(mockAdapterFactory.consumerExecutors.size() == 1);
    }

    @Test
    public void testCreateProducerAdapter() {
        MockAdapterFactory mockAdapterFactory = new MockAdapterFactory();
        ProducerAdapter producer = mockAdapterFactory.createProducerAdapter("");
        assertThat(producer, instanceOf(MockAdapter.class));
        assertTrue(mockAdapterFactory.consumerExecutors.size() == 0);
    }

    @Test
    public void testShutdown() {
        MockAdapterFactory mockAdapterFactory = new MockAdapterFactory();
        mockAdapterFactory.createConsumerAdapter("", true);
        assertTrue(mockAdapterFactory.consumerExecutors.size() == 1);
        mockAdapterFactory.shutdown();
        assertTrue(mockAdapterFactory.consumerExecutors.size() == 0);

    }

}
