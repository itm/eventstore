package de.uniluebeck.itm.eventstore;

import net.openhft.chronicle.tools.ChronicleTools;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Semaphore;

public class ChronicleBasedEventStorePerformanceTest {

	private static final int WRITE_ITERATIONS = 1000000;
	private static final int READ_ITERATIONS = 1000;
	private static final Semaphore semaphore = new Semaphore(0);
	private static Logger log = LoggerFactory.getLogger(ChronicleBasedEventStorePerformanceTest.class);

	static {
		BasicConfigurator.configure();
	}

	public static void main(String... args) {

		final EventStore<String> store = createEventStore();

		final long start = System.currentTimeMillis();
		final Random random = new Random(start);

		try {

			// writerThread will create 1 million entries and release the semaphore
			// readerThread will read 1000 times all entries from random start indices

			Thread writerThread = createWriterThread(store);
			Thread readerThread = createReaderThread(store, start, random);

			writerThread.start();
			readerThread.start();

			semaphore.acquire(2);

		} catch (InterruptedException e) {
			log.warn("Interrupt occurred.", e);
		}

		log.info("Test completed!");
	}

	private static Thread createReaderThread(final EventStore<String> store, final long start, final Random random) {
		return new Thread(new Runnable() {
			@Override
			public void run() {

				long time = System.nanoTime();
				long totalEntriesRead = 0;

				String dummy = "";
				for (int i = 0; i < READ_ITERATIONS; i++) {

					log.trace("\tread iteration = " + i);

					long randomTimestampInChronicle = start + random.nextInt((int) (System.currentTimeMillis() - start));

					Iterator<EventContainer<String>> iterator;
					try {
						iterator = store.getEventsFromTimestamp(randomTimestampInChronicle);
						while (iterator.hasNext()) {
							totalEntriesRead++;
							dummy = iterator.next().getEvent();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				time = System.nanoTime() - time;

				System.out.println(dummy);

				double seconds = (double) time / 1000000000;
				log.info("Reading finished! [TIME: {} ns, ENTRIES: {}, AVG: {} ns, ENTRIES/S: {}]",
						seconds,
						totalEntriesRead,
						time / totalEntriesRead,
						(double) totalEntriesRead / seconds
				);

				semaphore.release();
			}
		}, "Reader1"
		);
	}

	private static Thread createWriterThread(final EventStore<String> store) {
		return new Thread(new Runnable() {
			@Override
			public void run() {

				long time = System.nanoTime();
				try {
					for (int i = 0; i < WRITE_ITERATIONS; i++) {
						log.trace("\twrite iteration = " + i);
						store.storeEvent("Test" + i);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				time = System.nanoTime() - time;
				double seconds = (double) time / 1000000000;
				log.info("Writing finished! [TIME: {} ns, ITERATIONS: {}, AVG: {} ns, ENTRIES/S: {}]",
						time,
						WRITE_ITERATIONS,
						time / WRITE_ITERATIONS,
						(double) WRITE_ITERATIONS / seconds
				);
				semaphore.release();
			}
		}, "Writer"
		);
	}

	private static EventStore<String> createEventStore() {
		final String basePath = System.getProperty("java.io.tmpdir") + "/SimpleChronicle";
		ChronicleTools.deleteOnExit(basePath);
		try {

			return EventStoreFactory.<String>create()
					.eventStoreWithBasePath(basePath)
					.setSerializer(String::getBytes)
					.setDeserializer(String::new)
					.build();

		} catch (Exception e) {
			log.error("Exception while creating EventStore: ", e);
			throw new RuntimeException(e);
		}
	}

}
