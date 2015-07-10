package io.github.tcdl.msb.cli;

import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
        when(mockAdapterFactory.createConsumerAdapter(TOPIC_NAME)).thenReturn(mockConsumerAdapter);

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
        subscriptionManager.subscribe(TOPIC_NAME, mockMessageHandler);

        // verify that nothing happens
        verifyNoMoreInteractions(mockAdapterFactory);
        verifyNoMoreInteractions(mockAdapterFactory);
    }

    private void testInitialSubscription(String topicName) {
        // method under test
        subscriptionManager.subscribe(topicName, mockMessageHandler);

        verify(mockAdapterFactory).createConsumerAdapter(topicName);
        verify(mockConsumerAdapter).subscribe(mockMessageHandler);
    }
}