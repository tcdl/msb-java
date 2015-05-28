package io.github.tcdl.cli;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.mockito.Mockito.*;

public class CliMessageHandlerTest {
    @Test
    public void testSubscriptionToResponseQueue() {
        CliMessageHandlerSubscriber subscriber = Mockito.mock(CliMessageHandlerSubscriber.class);

        CliMessageHandler handler = new CliMessageHandler(subscriber, true, Collections.singletonList("response"));
        handler.onMessage(
                "{  \"topics\": {\n"
                        + "    \"to\": \"search:parsers:facets:v1\",\n"
                        + "    \"response\": \"search:parsers:facets:v1:response:3c3dec275b326c6500010843\"\n"
                        + "  }}"
        );

        Mockito.verify(subscriber).subscribe("search:parsers:facets:v1:response:3c3dec275b326c6500010843", handler);
    }

    @Test
    public void testNoSubscriptionIfMissingResponseQueue() {
        CliMessageHandlerSubscriber subscriber = Mockito.mock(CliMessageHandlerSubscriber.class);

        CliMessageHandler handler = new CliMessageHandler(subscriber, true, Collections.singletonList("response"));
        handler.onMessage(
                "{  \"topics\": {\n"
                        + "    \"to\": \"search:parsers:facets:v1\"\n"
                        + "  }}"
        );

        verifyNoMoreInteractions(subscriber);
    }

    @Test
    public void testNoSubscriptionIfNullResponseQueue() {
        CliMessageHandlerSubscriber subscriber = Mockito.mock(CliMessageHandlerSubscriber.class);

        CliMessageHandler handler = new CliMessageHandler(subscriber, true, Collections.singletonList("response"));
        handler.onMessage(
                "{  \"topics\": {\n"
                        + "    \"to\": \"search:parsers:facets:v1\",\n"
                        + "    \"response\": null\n"
                        + "  }}"
        );

        verifyNoMoreInteractions(subscriber);
    }

    @Test
    public void testSubscriptionNonExistingQueue() {
        CliMessageHandlerSubscriber subscriber = Mockito.mock(CliMessageHandlerSubscriber.class);
        CliMessageHandler handler = new CliMessageHandler(subscriber, true, Collections.singletonList("response"));

        Mockito.doThrow(new RuntimeException()).when(subscriber).subscribe("non-existent-queue", handler);

        handler.onMessage(
                "{  \"topics\": {\n"
                        + "    \"to\": \"search:parsers:facets:v1\",\n"
                        + "    \"response\": \"non-existent-queue\"\n"
                        + "  }}"
        );

        // The point of this test is to verify that no exception is thrown in such case
        // that's why we don't have any explicit assert or verification here
    }
}