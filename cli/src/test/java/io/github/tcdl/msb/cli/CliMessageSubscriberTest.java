package io.github.tcdl.msb.cli;

import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.ExchangeType;
import io.github.tcdl.msb.api.ResponderOptions;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class CliMessageSubscriberTest {

    public static final String TOPIC_NAME = "topic1";

    private AdapterFactory mockAdapterFactory;
    private ConsumerAdapter mockConsumerAdapter;
    private CliMessageHandler mockMessageHandler;

    private CliMessageSubscriber subscriptionManager;

    @Before
    public void setUp() {
        mockAdapterFactory = mock(AdapterFactory.class);
        mockConsumerAdapter = mock(ConsumerAdapter.class);
        when(mockAdapterFactory.createConsumerAdapter(eq(TOPIC_NAME), eq(false), any(ResponderOptions.class))).thenReturn(mockConsumerAdapter);

        mockMessageHandler = mock(CliMessageHandler.class);

        subscriptionManager = new CliMessageSubscriber(mockAdapterFactory);
    }

    @Test
    public void testInitialSubscriptionForTopic() {
        testInitialSubscription(TOPIC_NAME);
    }

    @Test
    public void testDuplicateSubscription() {
        testInitialSubscription(TOPIC_NAME);

        // make another subscription to the same topic
        subscriptionManager.subscribe(TOPIC_NAME, ExchangeType.FANOUT, mockMessageHandler);

        // verify that nothing happens
        verifyNoMoreInteractions(mockAdapterFactory);
        verifyNoMoreInteractions(mockAdapterFactory);
    }

    private void testInitialSubscription(String topicName) {
        // method under test
        subscriptionManager.subscribe(topicName, ExchangeType.FANOUT, mockMessageHandler);

        verify(mockAdapterFactory).createConsumerAdapter(eq(topicName), eq(false), any(ResponderOptions.class));
        verify(mockConsumerAdapter).subscribe(mockMessageHandler);
    }
}