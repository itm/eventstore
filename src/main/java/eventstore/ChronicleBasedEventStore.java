package eventstore;

import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.istack.internal.NotNull;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.NotSerializableException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;


public class ChronicleBasedEventStore implements IEventStore {
    private String chronicleBasePath;
    private IndexedChronicle chronicle;
    private static final int TIMESTAMP_SIZE = Long.SIZE / Byte.SIZE;
    private Object writeLock;

    private BiMap<Class<?>, Byte> mapping;
    private Map<Class<?>, Function<?, byte[]>> serializers;
    private Map<Byte, Function<byte[], ?>> deserializers;


    public ChronicleBasedEventStore(@NotNull String chronicleBasePath, Map<Class<?>, Function<?, byte[]>> serializers, Map<Class<?>, Function<byte[], ?>> deserializers) throws FileNotFoundException, IllegalArgumentException {
        this.chronicleBasePath = chronicleBasePath;
        this.writeLock = new Object();
        chronicle = new IndexedChronicle(chronicleBasePath);
        if (deserializers.size() != serializers.size() || serializers.size() > 256) {
            throw new IllegalArgumentException("There must be the same amount of serializers and deserializers. Furthermore only up to 256 serializers and deserializers are supported");
        }

        buildMaps(serializers, deserializers);


    }

    private void buildMaps(Map<Class<?>, Function<?, byte[]>> serializers, Map<Class<?>, Function<byte[], ?>> deserializers) {
        mapping = HashBiMap.create();
        this.serializers = serializers;
        this.deserializers = new HashMap<Byte, Function<byte[], ?>>();

        byte b = 0;
        for (Map.Entry<Class<?>, Function<byte[], ?>> entry : deserializers.entrySet()) {
            mapping.put(entry.getKey(),b);
            this.deserializers.put(b, entry.getValue());
            b++;
        }
    }

    @Override
    public <T> void storeEvent(@NotNull T object) throws IOException {
        synchronized (writeLock) {
            ExcerptAppender appender = chronicle.createAppender();
            Function<T, byte[]> serializer = (Function<T, byte[]>) serializers.get(object.getClass());
            try {
                byte[] serialized = serializer.apply(object);
                appender.startExcerpt(serialized.length + TIMESTAMP_SIZE + Byte.SIZE + Integer.SIZE);
                appender.writeLong(System.currentTimeMillis());
                appender.writeByte(mapping.get(object.getClass()));
                appender.write(serialized);
                appender.finish();
            } catch (NullPointerException e) {
                throw new NotSerializableException("Can't find a serializer for this type");
            }
        }
    }

    @Override
    public Iterator<IEventContainer<?>> getEventsBetweenTimestamps(long fromTime, long toTime) throws IOException {
        return new LimitedEventIterator(fromTime,toTime);
    }

    @Override
    public Iterator<IEventContainer<?>> getEventsFromTimestamp(long fromTime) throws IOException {
        return new InfiniteEventIterator(fromTime);
    }

    @Override
    public Iterator<IEventContainer<?>> getAllEvents() throws IOException {
        return new InfiniteEventIterator(0);
    }

    @Override
    public void close() {
        try {
            chronicle.close();
            chronicle = null;
        } catch (IOException e) {
            // Closing fails. Nothing to do.
        }
    }

    private abstract class AbstractEventIterator implements java.util.Iterator<IEventContainer<?>> {
        protected ExcerptTailer reader;
        protected IEventContainer next;

        public AbstractEventIterator(long fromTime) throws IOException {
            reader = chronicle.createTailer();
            if (fromTime > 0) {
                windToTimestamp(fromTime);
            } else {
                next = readNextEvent();
            }
        }

        protected boolean windToTimestamp(long timestamp) {
            long firstTimestamp;
            while (reader.nextIndex()) {
                firstTimestamp = reader.readLong();
                if (firstTimestamp >= timestamp) {
                    byte type = reader.readByte();
                    byte[] event = new byte[(int) reader.remaining()];
                    reader.readFully(event);
                    Function<byte[], ?> decoder = deserializers.get(type);
                    Object object = decoder.apply(event);

                    next = new DefaultEventContainer(object, timestamp);
                    return true;
                }
            }
            return false;
        }

        protected abstract IEventContainer<?> readNextEvent();


        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public IEventContainer<?> next() {
            if (next != null) {
                IEventContainer<?> event = next;
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
        }

        @Override
        protected IEventContainer<?> readNextEvent() {
            if (reader.nextIndex()) {
                long timestamp = reader.readLong();
                byte type = reader.readByte();
                byte[] event = new byte[(int) reader.remaining()];
                reader.readFully(event);
                Object object = deserializers.get(type).apply(event);
                return new DefaultEventContainer(object, timestamp);
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
        }

        @Override
        protected IEventContainer<?> readNextEvent() {
            if (reader.nextIndex()) {
                long timestamp = reader.readLong();
                if (timestamp > toTime) {
                    reader.finish();
                    return null;
                }
                byte type = reader.readByte();
                byte[] event = new byte[(int) reader.remaining()];
                reader.readFully(event);
                Object object = deserializers.get(type).apply(event);
                return new DefaultEventContainer(object, timestamp);
            }
            reader.finish();
            return null;
        }
    }
}
