package de.uniluebeck.itm.eventstore;

import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import de.uniluebeck.itm.eventstore.chronicle.IndexedChronicleAnalyzer;
import de.uniluebeck.itm.util.serialization.MultiClassSerializationHelper;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.NotSerializableException;
import java.util.Map;
import java.util.NoSuchElementException;


public class ChronicleBasedEventStore<T> implements IEventStore<T> {

    private static final int TIMESTAMP_SIZE = Long.SIZE / Byte.SIZE;
    private static Logger log = LoggerFactory.
            getLogger(ChronicleBasedEventStore.class);
    private final String chronicleBasePath;
    private final Object writeLock;
    private final Object closeControlLock = new Object();
    private final boolean readOnly;
    private IndexedChronicle chronicle;
    private int openCount = 0;

    private MultiClassSerializationHelper<T> serializationHelper;

    public ChronicleBasedEventStore(@Nonnull final String chronicleBasePath, final Map<Class<? extends T>, Function<? extends T, byte[]>> serializers,
                                    Map<Class<? extends T>, Function<byte[], ? extends T>> deserializers)
            throws IOException, IllegalArgumentException, ClassNotFoundException {
        this(chronicleBasePath, serializers, deserializers, false);

    }

    public ChronicleBasedEventStore(@Nonnull final String chronicleBasePath, final Map<Class<? extends T>, Function<? extends T, byte[]>> serializers,
                                    Map<Class<? extends T>, Function<byte[], ? extends T>> deserializers, boolean readOnly) throws IOException, IllegalArgumentException, ClassNotFoundException {
        this(chronicleBasePath, serializers, deserializers, readOnly, ChronicleConfig.SMALL.dataBlockSize());
    }

    public ChronicleBasedEventStore(@Nonnull final String chronicleBasePath, final Map<Class<? extends T>, Function<? extends T, byte[]>> serializers,
                                    Map<Class<? extends T>, Function<byte[], ? extends T>> deserializers, boolean readOnly, int dataBlockSize)
            throws IOException, IllegalArgumentException, ClassNotFoundException {
        this.writeLock = new Object();
        this.readOnly = readOnly;
        this.chronicleBasePath = chronicleBasePath;
        if (!readOnly) {
            incrementOpenCount();
        }
        try {
            ChronicleConfig config = ChronicleConfig.SMALL.clone().dataBlockSize(dataBlockSize);
            chronicle = new IndexedChronicle(chronicleBasePath, config);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("Can't create event store with base path " + chronicleBasePath);
        }

        if (deserializers.size() != serializers.size() || serializers.size() > 256) {
            throw new IllegalArgumentException(
                    "There must be the same amount of serializers and deserializers. Furthermore only up to 256 serializers and deserializers are supported"
            );
        }

        File mappingFile = new File(chronicleBasePath + ".mapping");
        BiMap<Class<? extends T>, Byte> mapping = MultiClassSerializationHelper.<T>loadOrCreateClassByteMap(serializers, deserializers, mappingFile);
        serializationHelper = new MultiClassSerializationHelper<T>(serializers, deserializers, mapping);


    }

    private static int entryOverhead() {
        return TIMESTAMP_SIZE + Byte.SIZE + Integer.SIZE;
    }

    private void incrementOpenCount() {
        synchronized (closeControlLock) {
            openCount++;
        }
    }

    @Override
    public void storeEvent(@Nonnull final T object) throws IOException, UnsupportedOperationException, IllegalArgumentException {
        storeEvent(object, System.currentTimeMillis());
    }

    @Override
    public void storeEvent(@Nonnull T object, long timestamp) throws IOException, UnsupportedOperationException, IllegalArgumentException {
        // Object is of type T, so Class is Class<T>. No need to check!
        @SuppressWarnings("unchecked") Class<T> c = (Class<T>) object.getClass();
        storeEvent(object, c, timestamp);
    }

    @Override
    public void storeEvent(@Nonnull final T object, final Class<T> type) throws IOException, UnsupportedOperationException, IllegalArgumentException {
        storeEvent(object, type, System.currentTimeMillis());
    }

    @Override
    public void storeEvent(@Nonnull T object, Class<T> type, long timestamp) throws IOException, UnsupportedOperationException, IllegalArgumentException {
        if (readOnly) {
            throw new UnsupportedOperationException("Storing events is not allowed in read only mode");
        }
        synchronized (writeLock) {

            try {
                byte[] serialized = serializationHelper.serialize(object, type);
                int entrySize = entryOverhead() + serialized.length;
                if (entrySize > chronicle.config().dataBlockSize()) {
                    throw new IllegalArgumentException("Object too big to be stored in event store. Actual size: "
                            + serialized.length + ", allowed size: " + (chronicle.config().dataBlockSize() - entryOverhead()));
                }
                ExcerptAppender appender = chronicle.createAppender();
                appender.startExcerpt(entrySize);
                appender.writeLong(timestamp);
                appender.write(serialized);
                appender.finish();
            } catch (NullPointerException e) {
                throw new NotSerializableException("Can't find a serializer for type " + type.getName());
            }
        }
    }

    @Override
    public CloseableIterator<IEventContainer<T>> getEventsBetweenTimestamps(long fromTime, long toTime) throws IOException {
        return new LimitedEventIterator(fromTime, toTime);
    }

    @Override
    public CloseableIterator<IEventContainer<T>> getEventsFromTimestamp(long fromTime) throws IOException {
        return new InfiniteEventIterator(fromTime);
    }

    @Override
    public CloseableIterator<IEventContainer<T>> getAllEvents() throws IOException {
        return new InfiniteEventIterator(0);
    }

    @Override
    public long actualPayloadByteSize() throws IOException {
        return new IndexedChronicleAnalyzer(this.chronicle).actualPayloadByteSize();
    }

    @Override
    public long size() {
        return chronicle.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void close() throws IOException {
        synchronized (closeControlLock) {
            openCount--;

            if (openCount == 0) {
                chronicle.close();
                chronicle = null;
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (chronicle != null) {
            chronicle.close();
            log.error("ChronicleBasedEventStore.finalize(): EventStore was still open. Have you forgotten closing it?");
        }
        super.finalize();
    }

    private abstract class AbstractEventIterator implements CloseableIterator<IEventContainer<T>> {

        protected ExcerptTailer reader;
        protected long fromTime;

        protected IEventContainer<T> next;

        public AbstractEventIterator(long fromTime) throws IOException {
            this.fromTime = fromTime;
            incrementOpenCount();
            reader = chronicle.createTailer();
        }

        @Override
        public void close() throws IOException {
            ChronicleBasedEventStore.this.close();
        }

        private boolean windToTimestamp(long timestamp) {
            long firstTimestamp;
            while (reader.nextIndex()) {
                firstTimestamp = reader.readLong();
                if (firstTimestamp >= timestamp) {
                    byte[] event = new byte[(int) reader.remaining()];
                    reader.readFully(event);
                    T object = serializationHelper.deserialize(event);

                    next = new DefaultEventContainer<T>(object, timestamp);
                    return true;
                }
            }
            return false;
        }

        protected abstract IEventContainer<T> readNextEvent();

        protected void finishSetup() {
            if (fromTime > 0) {
                windToTimestamp(fromTime);
            } else {
                next = readNextEvent();
            }
        }


        @Override
        public boolean hasNext() {
            if (next == null) {
                next = readNextEvent();
            }
            return next != null;
        }

        @Override
        public IEventContainer<T> next() {
            if (next != null) {
                IEventContainer<T> event = next;
                next = readNextEvent();
                return event;
            }

            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private class InfiniteEventIterator extends AbstractEventIterator {

        public InfiniteEventIterator(long fromTime) throws IOException {
            super(fromTime);
            finishSetup();
        }

        @Override
        protected IEventContainer<T> readNextEvent() {
            if (reader.nextIndex()) {
                long timestamp = reader.readLong();
                byte[] event = new byte[(int) reader.remaining()];
                reader.readFully(event);
                T object = serializationHelper.deserialize(event);
                return new DefaultEventContainer<T>(object, timestamp);
            }
            reader.finish();
            return null;
        }
    }

    /**
     * The limited event iterator is returned for iterating through events between two different timestamps
     */
    private class LimitedEventIterator extends AbstractEventIterator {

        private long toTime;

        public LimitedEventIterator(long fromTime, long toTime) throws IOException {
            super(fromTime);
            this.toTime = toTime;
            finishSetup();
        }

        @Override
        protected IEventContainer<T> readNextEvent() {
            if (reader.nextIndex()) {
                long timestamp = reader.readLong();
                if (timestamp > toTime) {
                    reader.finish();
                    return null;
                }
                byte[] event = new byte[(int) reader.remaining()];
                reader.readFully(event);
                T object = serializationHelper.deserialize(event);
                return new DefaultEventContainer<T>(object, timestamp);
            }
            reader.finish();
            return null;
        }
    }
}
