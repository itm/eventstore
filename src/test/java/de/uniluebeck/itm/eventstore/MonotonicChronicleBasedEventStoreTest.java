package de.uniluebeck.itm.eventstore;

import com.google.common.base.Function;
import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class MonotonicChronicleBasedEventStoreTest {

    private IEventStore store;

    @Before
    public void setUp() throws Exception {
        Map<Class<?>, Function<?, byte[]>> serializers = new HashMap<Class<?>, Function<?, byte[]>>();
        serializers.put(String.class, new Function<String, byte[]>() {
                    @Override
                    public byte[] apply(String string) {
                        return string.getBytes();
                    }

                    @Override
                    public String toString() {
                        return "String -> byte[]";
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

                    @Override
                    public String toString() {
                        return "byte[] -> String";
                    }
                }
        );

        serializers.put(BigInteger.class, new Function<BigInteger, byte[]>() {
                    @Override
                    public byte[] apply(BigInteger o) {
                        return o.toByteArray();
                    }

                    @Override
                    public String toString() {
                        return "BigInteger -> byte[]";
                    }
                }
        );

        deserializers.put(BigInteger.class, new Function<byte[], BigInteger>() {
                    @Override
                    public BigInteger apply(byte[] bytes) {
                        return new BigInteger(bytes);
                    }

                    @Override
                    public String toString() {
                        return "byte[] -> BigInteger";
                    }
                }
        );

        String basePath = System.getProperty("java.io.tmpdir") + "/SimpleChronicle";
        ChronicleTools.deleteOnExit(basePath);
        //noinspection unchecked
        store = EventStoreFactory.create().eventStoreWithBasePath(basePath).withSerializers(serializers).andDeserializers(deserializers).build();
    }

    @After
    public void cleanUp() {
        try {
            store.close();
        } catch (IOException e) {
            //
        }
    }

    @Test(expected = NotSerializableException.class)
    public void testInvalidStoreEvent() throws Exception {
        Object invalid = new Object();
        //noinspection unchecked
        store.storeEvent(invalid);
    }

    @Test
    public void testSingleEventReadFromTimestamp() throws Exception {
        String test = "Test";
        long start = System.currentTimeMillis();
        //noinspection unchecked
        store.storeEvent("Test");
        //noinspection unchecked
        CloseableIterator<IEventContainer<?>> iterator = store.getEventsFromTimestamp(start);
        assertNotNull(iterator);

        assertTrue(iterator.hasNext());
        IEventContainer<?> event = iterator.next();
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
            //noinspection unchecked
            store.storeEvent(BigInteger.valueOf(i));
        }
        Thread.sleep(1);
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i <= iteration; i++) {
            //noinspection unchecked
            store.storeEvent("Test" + i);
        }

        //noinspection unchecked
        CloseableIterator<IEventContainer<?>> iterator = store.getEventsFromTimestamp(timestamp);

        int index = 0;
        while (iterator.hasNext()) {
            IEventContainer<?> event = iterator.next();
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
        //noinspection unchecked
        iterator = store.getEventsFromTimestamp(System.currentTimeMillis());

        assertFalse(iterator.hasNext());

        iterator.close();
    }

    @Test
    public void testGetEventsBetweenTimestamps() throws Exception {
        int iteration = 10000;
        for (int i = 0; i <= iteration; i++) {
            //noinspection unchecked
            store.storeEvent(BigInteger.valueOf(i));
        }
        Thread.sleep(1);
        long from = System.currentTimeMillis();
        for (int i = 0; i <= iteration; i++) {
            //noinspection unchecked
            store.storeEvent("Between" + i);
        }
        long to = System.currentTimeMillis();
        Thread.sleep(1);
        for (int i = 0; i <= iteration; i++) {
            //noinspection unchecked
            store.storeEvent("After" + i);
        }
        //noinspection unchecked
        CloseableIterator<IEventContainer<?>> iterator = store.getEventsBetweenTimestamps(from, to);
        assertTrue(iterator.hasNext());
        int index = 0;
        while (iterator.hasNext()) {
            IEventContainer<?> event = iterator.next();
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
        //noinspection unchecked
        CloseableIterator<IEventContainer<?>> iterator = store.getAllEvents();
        assertFalse(iterator.hasNext());
        iterator.close();
    }

    @Test
    public void testGetAllEvents() throws Exception {
        int iteration = 10000;
        for (int i = 0; i <= iteration; i++) {
            //noinspection unchecked
            store.storeEvent(BigInteger.valueOf(i));
        }
        //noinspection unchecked
        CloseableIterator<IEventContainer<?>> iterator = store.getAllEvents();

        int index = 0;
        while (iterator.hasNext()) {
            IEventContainer<?> event = iterator.next();
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

        //noinspection unchecked
        store.storeEvent(a);
        //noinspection unchecked
        store.storeEvent(b);
        //noinspection unchecked
        store.storeEvent(c);

        //noinspection unchecked
        CloseableIterator<IEventContainer<?>> iterator = store.getAllEvents();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());
        IEventContainer<?> event = iterator.next();
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
            //noinspection unchecked
            store.storeEvent(BigInteger.valueOf(i));
            //noinspection unchecked
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
        //noinspection unchecked
        CloseableIterator<IEventContainer<?>> iterator = store.getAllEvents();

        int index = 0;
        while (iterator.hasNext()) {
            IEventContainer<?> event = iterator.next();
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
