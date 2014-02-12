package eventstore;

import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;

import javax.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.NotSerializableException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;


public class ChronicleBasedEventStore<T> implements IEventStore<T> {

	private IndexedChronicle chronicle;

	private static final int TIMESTAMP_SIZE = Long.SIZE / Byte.SIZE;

	private final Object writeLock;

	private BiMap<Class<T>, Byte> mapping;

	private Map<Class<T>, Function<T, byte[]>> serializers;

	private Map<Byte, Function<byte[], T>> deserializers;


	public ChronicleBasedEventStore(@Nonnull String chronicleBasePath, Map<Class<T>, Function<T, byte[]>> serializers,
									Map<Class<T>, Function<byte[], T>> deserializers)
			throws FileNotFoundException, IllegalArgumentException {
		this.writeLock = new Object();
		chronicle = new IndexedChronicle(chronicleBasePath);
		if (deserializers.size() != serializers.size() || serializers.size() > 256) {
			throw new IllegalArgumentException(
					"There must be the same amount of serializers and deserializers. Furthermore only up to 256 serializers and deserializers are supported"
			);
		}

		buildMaps(serializers, deserializers);


	}

	private void buildMaps(Map<Class<T>, Function<T, byte[]>> serializers,
						   Map<Class<T>, Function<byte[], T>> deserializers) {
		mapping = HashBiMap.create();
		this.serializers = serializers;
		this.deserializers = new HashMap<Byte, Function<byte[], T>>();

		byte b = 0;
		for (Map.Entry<Class<T>, Function<byte[], T>> entry : deserializers.entrySet()) {
			mapping.put(entry.getKey(), b);
			this.deserializers.put(b, entry.getValue());
			b++;
		}
	}

	@Override
	public void storeEvent(@Nonnull T object) throws IOException {
		synchronized (writeLock) {
			ExcerptAppender appender = chronicle.createAppender();
			// Object is of type T, so Class is Class<T>. No need to check!
			@SuppressWarnings("unchecked") Class<T> c = (Class<T>) object.getClass();
			Function<T, byte[]> serializer = serializers.get(c);
			try {
				byte[] serialized = serializer.apply(object);
				appender.startExcerpt(serialized.length + TIMESTAMP_SIZE + Byte.SIZE + Integer.SIZE);
				appender.writeLong(System.currentTimeMillis());
				appender.writeByte(mapping.get(c));
				appender.write(serialized);
				appender.finish();
			} catch (NullPointerException e) {
				throw new NotSerializableException("Can't find a serializer for this type");
			}
		}
	}

	@Override
	public Iterator<IEventContainer<T>> getEventsBetweenTimestamps(long fromTime, long toTime) throws IOException {
		return new LimitedEventIterator(fromTime, toTime);
	}

	@Override
	public Iterator<IEventContainer<T>> getEventsFromTimestamp(long fromTime) throws IOException {
		return new InfiniteEventIterator(fromTime);
	}

	@Override
	public Iterator<IEventContainer<T>> getAllEvents() throws IOException {
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

	private abstract class AbstractEventIterator implements java.util.Iterator<IEventContainer<T>> {

		protected ExcerptTailer reader;

		protected IEventContainer<T> next;

		public AbstractEventIterator(long fromTime) throws IOException {
			reader = chronicle.createTailer();
			if (fromTime > 0) {
				windToTimestamp(fromTime);
			} else {
				next = readNextEvent();
			}
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


		@Override
		public boolean hasNext() {
            if(next == null) {
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
