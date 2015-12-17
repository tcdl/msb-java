package io.github.tcdl.msb.api.message;

import static org.junit.Assert.assertTrue;
import java.time.Clock;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.config.MsbConfig;
import org.junit.Test;

/**
 * Created by ruslan on 17.12.15.
 */
public class MetaMessageTest {

    private Clock clock = Clock.systemDefaultZone();

    @Test
    public void testDurationIsPositivValue() {
        MsbConfig msbConf = new MsbConfig(ConfigFactory.load());
        MetaMessage metaMessage = new MetaMessage.Builder(0, clock.instant().minusMillis(1), msbConf.getServiceDetails(), clock).build();
        assertTrue(metaMessage.getDurationMs() > 0);
    }
}
