package de.uniluebeck.itm.eventstore;

import com.google.common.base.Function;
import net.openhft.chronicle.tools.ChronicleTools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class ChronicleBasedEventStorePerformanceTest {

	private static ChronicleBasedEventStore<String> store;

	private static final int WRITE_ITERATIONS = 10000000;

	private static final int READ_ITERATIONS = 10000000;

	private static boolean readFinished = false;

	private static boolean writeFinished = false;

	public static void main(String... args) {
		Map<Class<?>, Function<?, byte[]>> serializers = new HashMap<Class<?>, Function<?, byte[]>>();
		serializers.put(String.class, new Function<String, byte[]>() {
			@Override
			public byte[] apply(String string) {
				return string.getBytes();
			}
		}
		);
		Map<Class<?>, Function<byte[], ?>> deserializers = new HashMap<Class<?>, Function<byte[], ?>>();
		deserializers.put(String.class, new Function<byte[], String>() {
			@Override
			public String apply(byte[] bytes) {
				try {
					return new String(bytes, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					return null;
				}
			}
		}
		);

		serializers.put(BigInteger.class, new Function<BigInteger, byte[]>() {
			@Override
			public byte[] apply(BigInteger o) {
				return o.toByteArray();
			}
		}
		);

		deserializers.put(BigInteger.class, new Function<byte[], BigInteger>() {
			@Override
			public BigInteger apply(byte[] bytes) {
				return new BigInteger(bytes);
			}
		}
		);

		String basePath = System.getProperty("java.io.tmpdir") + "/SimpleChronicle";
		ChronicleTools.deleteOnExit(basePath);
		final long start = System.currentTimeMillis();
		final Random random = new Random(start);
		try {
			store = new ChronicleBasedEventStore(basePath, serializers, deserializers);
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						for (int i = 0; i < WRITE_ITERATIONS; i++) {
							System.out.println("\twrite iteration = " + i);
							store.storeEvent("Test" + i);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					System.out.println("Writing finished!");
				}
			}, "Writer"
			).start();

			new Thread(new Runnable() {
				@Override
				public void run() {
					for (int i = 0; i < READ_ITERATIONS; i++) {
						System.out.println("\tread iteration = " + i);
						int offset = random.nextInt((int) (System.currentTimeMillis() - start));

						Iterator<IEventContainer<String>> iterator = null;
						try {
							iterator = store.getEventsFromTimestamp(start + offset);
							while (iterator.hasNext()) {
								iterator.next().getEvent();
							}
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					System.out.println("Reading finished");
				}
			}, "Reader1"
			).start();

			while (!(writeFinished && readFinished)) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		} catch (FileNotFoundException e) {
			System.err.println("Can't create chronicle");
		}
	}

}
