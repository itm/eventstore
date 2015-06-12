package de.uniluebeck.itm.eventstore;

import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.math.BigInteger;

import static org.junit.Assert.*;


/**
 * FIXME fix tests when VanillaChronicle is working as expected and un-ignore
 */
@RunWith(JUnit4.class)
@Ignore
public class VanillaChronicleBasedEventStoreImplTest {

	private EventStore<String> store;

	@Before
	public void setUp() throws Exception {
		String basePath = System.getProperty("java.io.tmpdir") + "/CyclingChronicle";
		ChronicleTools.deleteOnExit(basePath);

		store = EventStoreFactory.<String>create().eventStoreWithBasePath(basePath)
				.setSerializer(String::getBytes)
				.setDeserializer(String::new)
				.setCycling(true)
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
	public void testSingleEventReadFromTimestamp() throws Exception {
		String test = "Test";
		long start = System.currentTimeMillis();

		store.storeEvent("Test");

		CloseableIterator<EventContainer<String>> iterator = store.getEventsFromTimestamp(start);
		assertNotNull(iterator);

		assertTrue(iterator.hasNext());
		EventContainer<?> event = iterator.next();
		assertNotNull(event);

		assertEquals("The deserialized event is different from original", test, event.getEvent());

		assertTrue(event.getTimestamp() >= start);

		assertFalse(iterator.hasNext());
		iterator.close();

	}


	@Test
	public void testGetEventsFromTimestamp() throws Exception {
		int iteration = 10000;
		for (int i = 0; i <= iteration; i++) {

			store.storeEvent(String.valueOf(i));
			assertEquals(i + 1, store.size());
		}
		Thread.sleep(1);
		long timestamp = System.currentTimeMillis();
		for (int i = 0; i <= iteration; i++) {

			store.storeEvent("Test" + i);
		}


		CloseableIterator<EventContainer<String>> iterator = store.getEventsFromTimestamp(timestamp);

		int index = 0;
		while (iterator.hasNext()) {
			EventContainer<?> event = iterator.next();
			assertTrue(event.getTimestamp() >= timestamp);
			assertNotNull(event);
			assertEquals("Test" + index, event.getEvent());
			index++;
		}
		index--;
		assertEquals(iteration, index);
		iterator.close();
		// Test for reading at end
		Thread.sleep(1);

		iterator = store.getEventsFromTimestamp(System.currentTimeMillis());

		assertFalse(iterator.hasNext());

		iterator.close();
	}

	@Test
	public void testGetEventsBetweenTimestamps() throws Exception {
		int iteration = 10000;
		for (int i = 0; i <= iteration; i++) {

			store.storeEvent(String.valueOf(i));
		}
		Thread.sleep(1);
		long from = System.currentTimeMillis();
		for (int i = 0; i <= iteration; i++) {

			store.storeEvent("Between" + i);
		}
		long to = System.currentTimeMillis();
		Thread.sleep(1);
		for (int i = 0; i <= iteration; i++) {

			store.storeEvent("After" + i);
		}

		CloseableIterator<EventContainer<String>> iterator = store.getEventsBetweenTimestamps(from, to);
		assertTrue(iterator.hasNext());
		int index = 0;
		while (iterator.hasNext()) {
			EventContainer<?> event = iterator.next();
			assertNotNull(event);
			assertTrue(event.getTimestamp() >= from);
			assertTrue(event.getTimestamp() <= to);
			assertEquals("Between" + index, event.getEvent());
			index++;
		}
		index--;
		assertEquals(iteration, index);
		iterator.close();

	}

	@Test
	public void testReadEmptyStore() throws Exception {

		CloseableIterator<EventContainer<String>> iterator = store.getAllEvents();
		assertFalse(iterator.hasNext());
		iterator.close();
	}

	@Test
	public void testGetAllEvents() throws Exception {
		int iteration = 10000;
		for (int i = 0; i <= iteration; i++) {

			store.storeEvent(String.valueOf(i));
		}

		CloseableIterator<EventContainer<String>> iterator = store.getAllEvents();

		int index = 0;
		while (iterator.hasNext()) {
			EventContainer<?> event = iterator.next();
			assertNotNull(event);
			BigInteger next = (BigInteger) event.getEvent();
			assertEquals(BigInteger.valueOf(index), next);
			index++;
		}
		index--;
		assertEquals(iteration, index);
		iterator.close();
	}

	@Test
	public void testGetAllEventsWithString() throws Exception {
		String a = "TestA";
		String b = "TestB";
		String c = "TestC";


		store.storeEvent(a);

		store.storeEvent(b);

		store.storeEvent(c);


		CloseableIterator<EventContainer<String>> iterator = store.getAllEvents();
		assertNotNull(iterator);
		assertTrue(iterator.hasNext());
		EventContainer<?> event = iterator.next();
		assertNotNull(event);
		assertEquals(a, event.getEvent());

		assertTrue(iterator.hasNext());
		event = iterator.next();
		assertNotNull(event);
		assertEquals(b, event.getEvent());

		assertTrue(iterator.hasNext());
		event = iterator.next();
		assertNotNull(event);
		assertEquals(c, event.getEvent());
		iterator.close();
	}

	@Test
	public void testGetAllDifferentType() throws Exception {
		int iteration = 10000;
		for (int i = 0; i <= iteration; i++) {

			store.storeEvent(String.valueOf(i));

			store.storeEvent("Test" + i);
		}
		testMultipleReaders(iteration);
		testMultipleReaders(iteration);

	}

	/**
	 * Helper method for testGetAllDifferentType.
	 * This method is called multiple times to test different reading operations
	 *
	 * @param iteration the number of iterations
	 * @throws Exception default
	 */
	private void testMultipleReaders(int iteration) throws Exception {

		CloseableIterator<EventContainer<String>> iterator = store.getAllEvents();

		int index = 0;
		while (iterator.hasNext()) {
			EventContainer<?> event = iterator.next();
			assertNotNull(event);
			BigInteger next = (BigInteger) event.getEvent();
			assertEquals(BigInteger.valueOf(index), next);

			assertTrue(iterator.hasNext());
			event = iterator.next();
			assertNotNull(event);
			assertEquals("Test" + index, event.getEvent());
			index++;
		}
		index--;
		assertEquals(iteration, index);
		iterator.close();
	}

}
