package de.uniluebeck.itm.eventstore;

import com.google.common.base.Function;
import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class NonMonotonicChronicleBasedEventStoreImplTest {

    private EventStore store;

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

        String basePath = System.getProperty("java.io.tmpdir") + "/SimpleNonMonotonicChronicle";
        ChronicleTools.deleteOnExit(basePath);
        //noinspection unchecked
        store = EventStoreFactory.create().eventStoreWithBasePath(basePath).withSerializers(serializers).andDeserializers(deserializers).havingMonotonicEventOrder(false).build();
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
            //noinspection unchecked
            store.storeEvent(BigInteger.valueOf(value), value);
        }

        //noinspection unchecked
        CloseableIterator<EventContainer> iterator = store.getEventsBetweenTimestamps(from, to);

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
            //noinspection unchecked
            store.storeEvent(BigInteger.valueOf(i));
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
            //noinspection unchecked
            store.storeEvent(BigInteger.valueOf(value), value);
        }

        //noinspection unchecked
        CloseableIterator<EventContainer> iterator = store.getEventsBetweenTimestamps(from, to);

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
