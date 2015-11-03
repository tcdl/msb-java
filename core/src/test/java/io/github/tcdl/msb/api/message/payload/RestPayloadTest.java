package io.github.tcdl.msb.api.message.payload;

import static org.junit.Assert.*;
import io.github.tcdl.msb.api.message.payload.RestPayload.Builder;

import org.junit.Test;


public class RestPayloadTest {
    @Test
    public void testEquals() {
        Builder bob = payloadBuilder();
        Builder sameBob = payloadBuilder();

        assertEquals(bob.build(), sameBob.build());
    }

    @Test
    public void testNotEquals() {
        Builder bob = payloadBuilder();
        
        assertFalse(bob.build().equals(null));
        assertFalse(bob.build().equals("something completely different"));
        
        assertNotEquals(bob.build(), payloadBuilder().withBody("another-body").build());
        assertNotEquals(bob.build(), payloadBuilder().withBodyBuffer("another-body-buffer".getBytes()).build());
        assertNotEquals(bob.build(), payloadBuilder().withHeaders("some-other-headers").build());
        assertNotEquals(bob.build(), payloadBuilder().withParams("some-other-params").build());
        assertNotEquals(bob.build(), payloadBuilder().withQuery("another-query").build());
        assertNotEquals(bob.build(), payloadBuilder().withStatusCode(200).build());
        assertNotEquals(bob.build(), payloadBuilder().withStatusMessage("found").build());
    }
    
    @Test
    public void testHashCode() throws Exception {
        Builder bob = payloadBuilder();
        
        assertEquals(bob.build().hashCode(), bob.build().hashCode());
        assertNotEquals(bob.build().hashCode(), payloadBuilder().withBody("another-body").build().hashCode());
        assertNotEquals(bob.build().hashCode(), payloadBuilder().withBodyBuffer("another-body-buffer".getBytes()).build().hashCode());
        assertNotEquals(bob.build().hashCode(), payloadBuilder().withHeaders("some-other-headers").build().hashCode());
        assertNotEquals(bob.build().hashCode(), payloadBuilder().withParams("some-other-params").build().hashCode());
        assertNotEquals(bob.build().hashCode(), payloadBuilder().withQuery("another-query").build().hashCode());
        assertNotEquals(bob.build().hashCode(), payloadBuilder().withStatusCode(200).build().hashCode());
        assertNotEquals(bob.build().hashCode(), payloadBuilder().withStatusMessage("found").build().hashCode());
    }

    private Builder<String, String, String, String> payloadBuilder() {
        return new RestPayload.Builder<String, String, String, String>()
            .withBody("some-body")
            .withBodyBuffer("some-body".getBytes())
            .withHeaders("headers")
            .withParams("params")
            .withQuery("a-query")
            .withStatusCode(404)
            .withStatusMessage("not-found");
    }
    
}
