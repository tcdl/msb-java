package io.github.tcdl.msb.cli;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Collections;

import io.github.tcdl.msb.api.ExchangeType;
import org.junit.Test;

public class CliMessageHandlerTest {
    @Test
    public void testSubscriptionToResponseQueue() {
        CliMessageSubscriber subscriber = mock(CliMessageSubscriber.class);

        CliMessageHandler handler = new CliMessageHandler(subscriber, Collections.singletonList("response"), true);
        handler.onMessage(
                "{  \"topics\": {\n"
                        + "    \"to\": \"search:parsers:facets:v1\",\n"
                        + "    \"response\": \"search:parsers:facets:v1:response:3c3dec275b326c6500010843\"\n"
                        + "  }}", null
        );

        verify(subscriber).subscribe("search:parsers:facets:v1:response:3c3dec275b326c6500010843", ExchangeType.FANOUT, handler);
    }

    @Test
    public void testNoSubscriptionIfMissingResponseQueue() {
        CliMessageSubscriber subscriber = mock(CliMessageSubscriber.class);

        CliMessageHandler handler = new CliMessageHandler(subscriber, Collections.singletonList("response"), true);
        handler.onMessage(
                "{  \"topics\": {\n"
                        + "    \"to\": \"search:parsers:facets:v1\"\n"
                        + "  }}", null
        );

        verifyNoMoreInteractions(subscriber);
    }

    @Test
    public void testNoSubscriptionIfNullResponseQueue() {
        CliMessageSubscriber subscriber = mock(CliMessageSubscriber.class);

        CliMessageHandler handler = new CliMessageHandler(subscriber, Collections.singletonList("response"), true);
        handler.onMessage(
                "{  \"topics\": {\n"
                        + "    \"to\": \"search:parsers:facets:v1\",\n"
                        + "    \"response\": null\n"
                        + "  }}", null
        );

        verifyNoMoreInteractions(subscriber);
    }

    @Test
    public void testSubscriptionNonExistingQueue() {
        CliMessageSubscriber subscriber = mock(CliMessageSubscriber.class);
        CliMessageHandler handler = new CliMessageHandler(subscriber, Collections.singletonList("response"), true);

        doThrow(new RuntimeException()).when(subscriber).subscribe("non-existent-queue", ExchangeType.FANOUT, handler);

        handler.onMessage(
                "{  \"topics\": {\n"
                        + "    \"to\": \"search:parsers:facets:v1\",\n"
                        + "    \"response\": \"non-existent-queue\"\n"
                        + "  }}", null
        );

        // The point of this test is to verify that no exception is thrown in such case
        // that's why we don't have any explicit assert or verification here
    }
}