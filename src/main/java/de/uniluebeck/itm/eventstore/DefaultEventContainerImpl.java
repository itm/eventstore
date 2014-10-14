package de.uniluebeck.itm.eventstore;

public class DefaultEventContainerImpl<T> implements EventContainer<T> {

    private T event;

    private long timestamp;

    public DefaultEventContainerImpl(T event, long time) {
        this.event = event;
        this.timestamp = time;
    }

    @Override
    public T getEvent() {
        return event;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

}
