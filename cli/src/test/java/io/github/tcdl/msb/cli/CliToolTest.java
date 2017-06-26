package io.github.tcdl.msb.cli;

import io.github.tcdl.msb.api.ExchangeType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CliToolTest {
    @Test
    public void testStrToArr() {
        String[] args = strToArr("1 2 3");
        assertArrayEquals(new String[] { "1", "2", "3" }, args);

        args = strToArr("--topic search:parsers:facets:v1");
        assertArrayEquals(new String[] {"--topic", "search:parsers:facets:v1"}, args);
    }

    @Test
    public void testGetOptionAsBoolean() {
        boolean optionValue = CliTool.getOptionAsBoolean(strToArr("--pretty true"), "--pretty", "-p");
        assertTrue(optionValue);

        optionValue = CliTool.getOptionAsBoolean(strToArr("-p false"), "--pretty", "-p");
        assertFalse(optionValue);

        optionValue = CliTool.getOptionAsBoolean(strToArr("-p blah"), "--pretty", "-p");
        assertFalse(optionValue);
    }

    @Test
    public void testGetOptionAsListSingleValue() {
        List<String> optionValues = CliTool.getOptionAsList(strToArr("--topic search:parsers:facets:v1"), "--topic", "-t");
        assertNotNull(optionValues);
        assertEquals(1, optionValues.size());
        assertTrue(optionValues.contains("search:parsers:facets:v1"));
    }

    @Test
    public void testGetOptionAsListMultipleValues() {
        List<String> optionValues = CliTool.getOptionAsList(strToArr("--topic search:parsers:facets:v1,search:packages:v1"), "--topic", "-t");
        assertNotNull(optionValues);
        assertEquals(2, optionValues.size());
        assertTrue(optionValues.contains("search:parsers:facets:v1"));
        assertTrue(optionValues.contains("search:packages:v1"));
    }

    @Test
    public void testGetOptionAsListNoValues() {
        List<String> optionValues = CliTool.getOptionAsList(strToArr("a b c"), "--topic", "-t");
        assertNull(optionValues);
    }

    @Test
    public void testGetFollowDefaultValue() {
        List<String> follow = CliTool.getFollow(strToArr("--topic search:parsers:facets:v1"));
        assertNotNull(follow);
        assertEquals(1, follow.size());
        assertTrue(follow.contains("response"));
    }

    @Test
    public void testGetTopicsDefaultValue() {
        List<String> topics = CliTool.getTopics(strToArr("--follow response,ack"));
        assertNull(topics);
    }

    @Test
    public void getPrettyOutputDefaultValue() {
        boolean prettyOutput = CliTool.getPrettyOutput(strToArr("--follow response,ack"));
        assertTrue(prettyOutput);
    }

    @Test
    public void testSubscribe() {
        CliMessageSubscriber subscriptionManager = mock(CliMessageSubscriber.class);
        List<MsbExchange> topics = Arrays.asList(
                new MsbExchange("topic1", ExchangeType.FANOUT),
                new MsbExchange("topic2", ExchangeType.TOPIC));
        List<String> follow = Collections.singletonList("response");

        CliTool.subscribe(subscriptionManager, topics, follow, false);

        verify(subscriptionManager).subscribe(eq("topic1"), eq(ExchangeType.FANOUT), any(CliMessageHandler.class));
        verify(subscriptionManager).subscribe(eq("topic2"), eq(ExchangeType.TOPIC), any(CliMessageHandler.class));
    }

    private String[] strToArr(String argString) {
        return argString.split(" ");
    }
}