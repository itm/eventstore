package eventstore;

/**
 * The Event Container is returned by the iterator of an IEventStore. It contains the event itself and a timestamp
 */
public interface IEventContainer<T> {

    public T getEvent();
    public long getTimestamp();
}
