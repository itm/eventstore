package de.uniluebeck.itm.eventstore;

import com.google.common.base.Function;
import net.openhft.chronicle.ChronicleConfig;

import java.io.IOException;
import java.util.Map;

public class EventStoreFactory<T> {


    private EventStoreConfig<T> config;
    private EventStoreFactory() {
        config = new EventStoreConfig<T>();
        config.setReadOnly(false);
        config.setMonotonic(true);
        config.setDataBlockSize(ChronicleConfig.SMALL.dataBlockSize());
    }


    public static <T> EventStoreFactory create() {
        return new EventStoreFactory<T>();
    }


    public EventStoreFactory<T> chronicleBasePath(String chronicleBasePath) {
        config.setChronicleBasePath(chronicleBasePath);
        return this;
    }

    public EventStoreFactory<T> readOnly(boolean readOnly) {
        config.setReadOnly(readOnly);
        return this;
    }

    public EventStoreFactory<T> serializers(Map<Class<? extends T>, Function<? extends T, byte[]>> serializers) {
        config.setSerializers(serializers);
        return this;
    }

    public EventStoreFactory<T> deserializers(Map<Class<? extends T>, Function<byte[], ? extends T>> deserializers) {
        config.setDeserializers(deserializers);
        return this;
    }

    public EventStoreFactory<T> dataBlockSize(int dataBlockSize) {
        config.setDataBlockSize(dataBlockSize);
        return this;
    }

    public EventStoreFactory<T> monotonic(boolean monotonic) {
        config.setMonotonic(monotonic);
        return this;
    }

    public IEventStore<T> build() throws IllegalArgumentException, IOException, ClassNotFoundException {
        if (config.isValid()) {
            return new ChronicleBasedEventStore<T>(config);
        } else {
            return null;
        }

    }

}
