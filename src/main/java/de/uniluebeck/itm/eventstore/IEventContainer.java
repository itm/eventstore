package de.uniluebeck.itm.eventstore;

/**
 * The Event Container is returned by the iterator of an IEventStore. It contains the event itself and a timestamp
 */
public interface IEventContainer<T> {

    /**
     * Returns the actual data of this object
     * @return the object
     */
	T getEvent();

    /**
     * Returns the timestamp stored beside this event
     * @return the events timestamp
     */
	long getTimestamp();
}
