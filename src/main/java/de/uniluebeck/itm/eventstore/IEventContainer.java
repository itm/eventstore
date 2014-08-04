package de.uniluebeck.itm.eventstore;

/**
 * The Event Container is returned by the iterator of an IEventStore. It contains the event itself and a timestamp
 */
public interface IEventContainer<T> {

	T getEvent();

	long getTimestamp();
}
