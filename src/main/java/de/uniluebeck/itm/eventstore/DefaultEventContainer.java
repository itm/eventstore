package de.uniluebeck.itm.eventstore;

public class DefaultEventContainer<T> implements IEventContainer<T> {

    private T event;

    private long timestamp;

    public DefaultEventContainer(T event, long time) {
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
