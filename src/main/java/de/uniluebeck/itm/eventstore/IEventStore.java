package de.uniluebeck.itm.eventstore;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;

/**
 * This interface contains all methods needed for using the event store
 */
public interface IEventStore<T> extends Closeable {

	/**
	 * Method for storing an object
	 *
	 * @param object
	 * 		an object to store
	 *
	 * @throws java.io.IOException
	 * 		if the stream is broken or the event couldn't be serialized
	 */
	public void storeEvent(@Nonnull final T object) throws IOException;

    public void storeEvent(@Nonnull final T object, final Class<T> type) throws  IOException;

	/**
	 * Getting an iterator for events between two timestamps
	 *
	 * @param fromTime
	 * 		the start time (inclusive)
	 * @param toTime
	 * 		the end time (inclusive)
	 *
	 * @return an iterator for sequential read access
	 *
	 * @throws java.io.IOException
	 * 		if the underlying stream is broken
	 */
	public CloseableIterator<IEventContainer<T>> getEventsBetweenTimestamps(long fromTime, long toTime) throws IOException;

	/**
	 * Getting an iterator for events from a given timestamp until the last event in the storage
	 *
	 * @param fromTime
	 * 		the start time (inclusive)
	 *
	 * @return an iterator for sequential read access
	 *
	 * @throws java.io.IOException
	 * 		if the underlying stream is broken
	 */
	public CloseableIterator<IEventContainer<T>> getEventsFromTimestamp(long fromTime) throws IOException;


	/**
	 * Getting an iterator for all events in the storage
	 *
	 * @return an iterator for sequential read access starting with the first event in the storage
	 *
	 * @throws IOException
	 * 		if the underlying stream is broken
	 */
	public CloseableIterator<IEventContainer<T>> getAllEvents() throws IOException;

}