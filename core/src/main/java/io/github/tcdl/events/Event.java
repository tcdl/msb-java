package io.github.tcdl.events;

/**
 * Created by rdro on 4/23/2015.
 */
public class Event {

    private String name;

    public Event(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Event event = (Event) o;

        return name.equals(event.name);

    }

    @Override public int hashCode() {
        return name.hashCode();
    }
}
