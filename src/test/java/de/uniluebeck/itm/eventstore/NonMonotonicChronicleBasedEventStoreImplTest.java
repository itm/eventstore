package de.uniluebeck.itm.eventstore;

import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class NonMonotonicChronicleBasedEventStoreImplTest {

	private EventStore<String> store;

	@Before
	public void setUp() throws Exception {
		String basePath = System.getProperty("java.io.tmpdir") + "/SimpleNonMonotonicChronicle";
		ChronicleTools.deleteOnExit(basePath);

		store = EventStoreFactory.<String>create().eventStoreWithBasePath(basePath)
				.setSerializer(String::getBytes)
				.setDeserializer(String::new)
				.havingMonotonicEventOrder(false)
				.build();
	}

	@After
	public void cleanUp() {
		try {
			store.close();
		} catch (IOException e) {
			//
		}
	}

	@Test
	public void testEventsFoundIfOutOfOrder() throws Exception {
		long from = 1;
		long to = 20;
		int[] values = {1, 2, 3, 100, 110, 70, 75, 5, 9, 20, 30};

		for (int value : values) {

			store.storeEvent(String.valueOf(value), value);
		}


		CloseableIterator<EventContainer<String>> iterator = store.getEventsBetweenTimestamps(from, to);

		for (int expected : values) {
			if (expected > to) {
				continue;
			}
			assertTrue(iterator.hasNext());
			EventContainer container = iterator.next();
			assertNotNull(container);
			assertEquals(expected, container.getTimestamp());
		}

		assertFalse(iterator.hasNext());
		iterator.close();
	}

	@Test
	public void testSizeAndEmpty() throws Exception {
		assertEquals(0, store.size());
		assertTrue(store.isEmpty());

		for (int i = 0; i < 100; i++) {

			store.storeEvent(String.valueOf(i));
			assertEquals(i + 1, store.size());
		}
		assertFalse(store.isEmpty());
	}

	@Test
	public void testEventsFoundIfInOrder() throws Exception {
		long from = 1;
		long to = 20;
		int[] values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 17, 17, 20, 50, 70};

		for (int value : values) {

			store.storeEvent(String.valueOf(value), value);
		}


		CloseableIterator<EventContainer<String>> iterator = store.getEventsBetweenTimestamps(from, to);

		for (int expected : values) {
			if (expected > to) {
				continue;
			}
			assertTrue(iterator.hasNext());
			EventContainer container = iterator.next();
			assertNotNull(container);
			assertEquals(expected, container.getTimestamp());
		}

		assertFalse(iterator.hasNext());
		iterator.close();
	}


}
