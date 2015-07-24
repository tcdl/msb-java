package io.github.tcdl.msb.api.message.payload;

import static org.junit.Assert.*;
import io.github.tcdl.msb.api.message.payload.Payload.Builder;

import org.junit.Test;


public class PayloadTest {

    @Test
    public void testEqualsImpl() {
        Builder bob = payload();
        
        assertEquals(bob.build(), bob.build());
        
        assertFalse(bob.build().equals(null));
        assertFalse(bob.build().equals("something completely different"));
        
        assertNotEquals(bob.build(), payload().withBody("another-body").build());
        assertNotEquals(bob.build(), payload().withBodyBuffer("another-body-buffer").build());
        assertNotEquals(bob.build(), payload().withHeaders("some-other-headers").build());
        assertNotEquals(bob.build(), payload().withParams("some-other-params").build());
        assertNotEquals(bob.build(), payload().withQuery("another-query").build());
        assertNotEquals(bob.build(), payload().withStatusCode(200).build());
        assertNotEquals(bob.build(), payload().withStatusMessage("found").build());
    }
    
    @Test
    public void testHashCodeImpl() throws Exception {
        Builder bob = payload();
        
        assertEquals(bob.build().hashCode(), bob.build().hashCode());
        assertNotEquals(bob.build().hashCode(), payload().withBody("another-body").build().hashCode());
        assertNotEquals(bob.build().hashCode(), payload().withBodyBuffer("another-body-buffer").build().hashCode());
        assertNotEquals(bob.build().hashCode(), payload().withHeaders("some-other-headers").build().hashCode());
        assertNotEquals(bob.build().hashCode(), payload().withParams("some-other-params").build().hashCode());
        assertNotEquals(bob.build().hashCode(), payload().withQuery("another-query").build().hashCode());
        assertNotEquals(bob.build().hashCode(), payload().withStatusCode(200).build().hashCode());
        assertNotEquals(bob.build().hashCode(), payload().withStatusMessage("found").build().hashCode());
    }

    private Builder<String, String, String, String> payload() {
        return new Payload.Builder<String, String, String, String>()
            .withBody("some-body")
            .withBodyBuffer("a-body-buffer")
            .withHeaders("headers")
            .withParams("params")
            .withQuery("a-query")
            .withStatusCode(404)
            .withStatusMessage("not-found");
    }
    
}
