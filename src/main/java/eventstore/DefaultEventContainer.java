package eventstore;

/**
 * Created by Tobi on 29.01.14.
 */
public class DefaultEventContainer<T> implements IEventContainer{
    private T event;
    private long timestamp;

    DefaultEventContainer(T event, long time) {
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
