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
     * @param object an object to store
     * @throws java.io.IOException                     if the stream is broken or the event couldn't be serialized
     * @throws java.lang.UnsupportedOperationException if the event store is in read only mode
     * @throws java.lang.IllegalArgumentException      if the provided objects serialized form is larger than the block size of this event store
     */
    public void storeEvent(@Nonnull final T object) throws IOException, UnsupportedOperationException, IllegalArgumentException;


    /**
     * Method for storing an object
     *
     * @param object an object to store
     * @param type   the type for which the serializer is stored.
     * @throws java.io.IOException                     if the stream is broken or the event couldn't be serialized
     * @throws java.lang.UnsupportedOperationException if the event store is in read only mode
     * @throws java.lang.IllegalArgumentException      if the provided objects serialized form is larger than the block size of this event store
     */
    public void storeEvent(@Nonnull final T object, final Class<T> type) throws IOException, UnsupportedOperationException, IllegalArgumentException;

    /**
     * Getting an iterator for events between two timestamps
     *
     * @param fromTime the start time (inclusive)
     * @param toTime   the end time (inclusive)
     * @return an iterator for sequential read access
     * @throws java.io.IOException if the underlying stream is broken
     */
    public CloseableIterator<IEventContainer<T>> getEventsBetweenTimestamps(long fromTime, long toTime) throws IOException;

    /**
     * Getting an iterator for events from a given timestamp until the last event in the storage
     *
     * @param fromTime the start time (inclusive)
     * @return an iterator for sequential read access
     * @throws java.io.IOException if the underlying stream is broken
     */
    public CloseableIterator<IEventContainer<T>> getEventsFromTimestamp(long fromTime) throws IOException;


    /**
     * Getting an iterator for all events in the storage
     *
     * @return an iterator for sequential read access starting with the first event in the storage
     * @throws IOException if the underlying stream is broken
     */
    public CloseableIterator<IEventContainer<T>> getAllEvents() throws IOException;


    /**
     * @see de.uniluebeck.itm.eventstore.chronicle.IndexedChronicleAnalyzer#actualPayloadByteSize() for a description
     */
    public long actualPayloadByteSize() throws IOException;

    /**
     * Getter for the number of entries in this store
     * @return the number of events in this store
     */
    public long size();

}