package eventstore;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Iterator;

/**
 * This interface contains all methods needed for using the event store
 */
public interface IEventStore<T> {

	/**
	 * Method for storing an object
	 *
	 * @param object
	 * 		an object to store
	 *
	 * @throws java.io.IOException
	 * 		if the stream is broken or the event couldn't be serialized
	 */
	public void storeEvent(@Nonnull T object) throws IOException;

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
	public Iterator<IEventContainer<T>> getEventsBetweenTimestamps(long fromTime, long toTime) throws IOException;

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
	public Iterator<IEventContainer<T>> getEventsFromTimestamp(long fromTime) throws IOException;


	/**
	 * Getting an iterator for all events in the storage
	 *
	 * @return an iterator for sequential read access starting with the first event in the storage
	 *
	 * @throws IOException
	 * 		if the underlying stream is broken
	 */
	public Iterator<IEventContainer<T>> getAllEvents() throws IOException;


	/**
	 * Closes the event store. Later write or read operations will fail.
	 */
	public void close();

}