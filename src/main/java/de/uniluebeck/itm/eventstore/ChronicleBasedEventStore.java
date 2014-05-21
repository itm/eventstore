package de.uniluebeck.itm.eventstore;

import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.*;


public class ChronicleBasedEventStore<T> implements IEventStore<T> {
    public static final String SERIALIZER_MAP_FILE_EXTENSION = ".mapping";
    private static final int TIMESTAMP_SIZE = Long.SIZE / Byte.SIZE;
    private static Logger log = LoggerFactory.
            getLogger(ChronicleBasedEventStore.class);
    private final String chronicleBasePath;
    private final Object writeLock;
    private final Object closeControlLock = new Object();
    private final boolean readOnly;
    private IndexedChronicle chronicle;
    private BiMap<Class<T>, Byte> mapping;
    private Map<Class<T>, Function<T, byte[]>> serializers;
    private Map<Byte, Function<byte[], T>> deserializers;
    private int openCount = 0;

    public ChronicleBasedEventStore(@Nonnull final String chronicleBasePath, final Map<Class<T>, Function<T, byte[]>> serializers,
                                    Map<Class<T>, Function<byte[], T>> deserializers)
            throws FileNotFoundException, IllegalArgumentException, ClassNotFoundException {
        this(chronicleBasePath, serializers, deserializers, false);

    }

    public ChronicleBasedEventStore(@Nonnull final String chronicleBasePath, final Map<Class<T>, Function<T, byte[]>> serializers,
                                    Map<Class<T>, Function<byte[], T>> deserializers, boolean readOnly)
            throws FileNotFoundException, IllegalArgumentException, ClassNotFoundException {
        this.writeLock = new Object();
        this.readOnly = readOnly;
        this.chronicleBasePath = chronicleBasePath;
        if (!readOnly) {
            incrementOpenCount();
        }
        try {
            chronicle = new IndexedChronicle(chronicleBasePath);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("Can't create event store with base path " + chronicleBasePath);
        }

        if (deserializers.size() != serializers.size() || serializers.size() > 256) {
            throw new IllegalArgumentException(
                    "There must be the same amount of serializers and deserializers. Furthermore only up to 256 serializers and deserializers are supported"
            );
        }

        buildMaps(serializers, deserializers);


    }


    private void incrementOpenCount() {
        synchronized (closeControlLock) {
            openCount++;
        }
    }

    private void buildMaps(Map<Class<T>, Function<T, byte[]>> serializers,
                           Map<Class<T>, Function<byte[], T>> externalDeserializers) throws ClassNotFoundException, IllegalArgumentException, FileNotFoundException {


        HashMap<String, Byte> serializerMapping = loadPersistedSerializerMapping();
        mapping = HashBiMap.create();
        Byte maxByte = Byte.MIN_VALUE;
        this.serializers = serializers;
        this.deserializers = new HashMap<Byte, Function<byte[], T>>();

        // Build mapping for existing types
        List<String> notFoundClasses = new ArrayList<String>();
        for (Map.Entry<String, Byte> entry : serializerMapping.entrySet()) {
            Byte id = entry.getValue();
            String className = entry.getKey();
            try {
                Class clazz = Class.forName(className);
                mapping.put(clazz, id);
                if (id > maxByte) {
                    maxByte = id;
                }
            } catch (ClassNotFoundException e) {
                notFoundClasses.add(className);
            }
        }

        if (notFoundClasses.size() > 0) {
            throw new ClassNotFoundException("Unknown classes in event store mapping file. Check the mapping file and fix the class names if appropriate.\nUnknown Classes: " + notFoundClasses);
        }


        byte b = maxByte;
        if (serializers.size() > 0) {
            if (b < Byte.MAX_VALUE - serializers.size()) {
                b = (byte) (b + 1);
                for (Map.Entry<Class<T>, Function<byte[], T>> entry : externalDeserializers.entrySet()) {
                    String className = entry.getKey().getName();
                    if (!serializerMapping.containsKey(className)) {
                        serializerMapping.put(className, b);
                        mapping.put(entry.getKey(), b);
                        this.deserializers.put(b, entry.getValue());
                        b++;
                    } else {
                        this.deserializers.put(serializerMapping.get(className), entry.getValue());
                    }
                }


            } else {
                throw new IllegalArgumentException("Can't add that many serializers and deserializers!");
            }
        }

        if (!storeSerializerMapping(serializerMapping)) {
            throw new FileNotFoundException("Event Store can't create mapping file or file is not writable!: " + (chronicleBasePath + SERIALIZER_MAP_FILE_EXTENSION));
        }
    }

    private HashMap<String, Byte> loadPersistedSerializerMapping() throws FileNotFoundException {
        File serializerMappingFile = new File(chronicleBasePath + SERIALIZER_MAP_FILE_EXTENSION);
        HashMap<String, Byte> serializerMapping = new HashMap<String, Byte>();
        if (serializerMappingFile.exists()) {
            BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(serializerMappingFile));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] components = line.split(",");
                    if (components.length != 2) {
                        log.warn("Invalid line in mapping file: {}", line);
                        continue;
                    }
                    serializerMapping.put(components[0], Byte.parseByte(components[1]));
                }
            } catch (FileNotFoundException e) {
                log.error("Can't find mapping file {}.", serializerMappingFile.getAbsolutePath());
                throw new FileNotFoundException("Event Store can't find mapping file " + serializerMappingFile.getAbsolutePath());
            } catch (IOException e) {
                log.error("Failed to read line from CSV mapping file.", e);
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                    }
                }
            }

        }

        return serializerMapping;
    }

    private boolean storeSerializerMapping(HashMap<String, Byte> serializerMapping) {
        File serializerMappingFile = new File(chronicleBasePath + SERIALIZER_MAP_FILE_EXTENSION);
        if (!serializerMappingFile.exists()) {
            try {
                serializerMappingFile.createNewFile();
            } catch (IOException e) {
                log.error("Can't create serializer mapping file.", e);
                return false;
            }
        }
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(serializerMappingFile));
            Iterator<Map.Entry<String, Byte>> it = serializerMapping.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Byte> entry = it.next();
                bw.write(entry.getKey() + "," + entry.getValue());
                if (it.hasNext()) {
                    bw.write("\n");
                }
            }
        } catch (IOException e) {
            log.error("Can't create file writer for mapping file.", e);
            return false;
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                }
            }
        }

        return true;
    }

    @Override
    public void storeEvent(@Nonnull final T object) throws IOException, UnsupportedOperationException {
         // Object is of type T, so Class is Class<T>. No need to check!
        @SuppressWarnings("unchecked") Class<T> c = (Class<T>) object.getClass();
        storeEvent(object, c);

    }

    @Override
    public void storeEvent(@Nonnull final T object, final Class<T> type) throws IOException, UnsupportedOperationException {
        if (readOnly) {
            throw new UnsupportedOperationException("Storing events is not allowed in read only mode");
        }
        synchronized (writeLock) {

            ExcerptAppender appender = chronicle.createAppender();
            Function<T, byte[]> serializer = serializers.get(type);
            try {
                byte[] serialized = serializer.apply(object);
                appender.startExcerpt(serialized.length + TIMESTAMP_SIZE + Byte.SIZE + Integer.SIZE);
                appender.writeLong(System.currentTimeMillis());
                appender.writeByte(mapping.get(type));
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
                    byte type = reader.readByte();
                    byte[] event = new byte[(int) reader.remaining()];
                    reader.readFully(event);
                    Function<byte[], T> decoder = deserializers.get(type);
                    T object = decoder.apply(event);

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
                byte type = reader.readByte();
                byte[] event = new byte[(int) reader.remaining()];
                reader.readFully(event);
                T object = deserializers.get(type).apply(event);
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
                byte type = reader.readByte();
                byte[] event = new byte[(int) reader.remaining()];
                reader.readFully(event);
                T object = deserializers.get(type).apply(event);
                return new DefaultEventContainer<T>(object, timestamp);
            }
            reader.finish();
            return null;
        }
    }
}
