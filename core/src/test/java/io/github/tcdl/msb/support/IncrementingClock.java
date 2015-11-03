package io.github.tcdl.msb.support;

import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.TemporalUnit;

public class IncrementingClock extends Clock implements Serializable {
    private static final long serialVersionUID = 4423672464736353647L;
    private Instant instant;
    private final ZoneId zone;
    final long step;
    final TemporalUnit unit;

    public IncrementingClock(Instant fixedInstant, ZoneId zone, long step, TemporalUnit unit) {
        this.instant = fixedInstant;
        this.zone = zone;
        this.step = step;
        this.unit = unit;
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        if (zone.equals(this.zone)) {
            return this;
        }
        return new IncrementingClock(instant, zone, step, unit);
    }

    @Override
    public long millis() {
        instant = instant.plus(step, unit);
        return instant.toEpochMilli();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        IncrementingClock that = (IncrementingClock) o;

        if (step != that.step)
            return false;
        if (!instant.equals(that.instant))
            return false;
        if (!zone.equals(that.zone))
            return false;
        return unit.equals(that.unit);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + instant.hashCode();
        result = 31 * result + zone.hashCode();
        result = 31 * result + (int) (step ^ (step >>> 32));
        result = 31 * result + unit.hashCode();
        return result;
    }
}
