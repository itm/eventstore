package de.uniluebeck.itm.eventstore;

import com.google.common.base.Function;
import de.uniluebeck.itm.eventstore.adapter.ChronicleAdapter;
import de.uniluebeck.itm.eventstore.adapter.IndexedChronicleAdapterImpl;
import de.uniluebeck.itm.eventstore.adapter.VanillaChronicleAdapterImpl;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.VanillaChronicleConfig;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class EventStoreFactory<T> {


    private EventStoreConfig<T> config;

    private EventStoreFactory() {
        config = new EventStoreConfig<T>();
    }


    public static <T> EventStoreFactory create() {
        return new EventStoreFactory<T>();
    }


    public EventStoreFactory<T> eventStoreWithBasePath(String chronicleBasePath) {
        config.setChronicleBasePath(chronicleBasePath);
        return this;
    }

    public EventStoreFactory<T> inReadOnlyMode(boolean readOnly) {
        config.setReadOnly(readOnly);
        return this;
    }

    public EventStoreFactory<T> setChronicleConfig(Object chronicleConfig) throws IllegalArgumentException {
        if (chronicleConfig instanceof VanillaChronicleConfig) {
            config.setVanillaChronicleConfig((VanillaChronicleConfig) chronicleConfig);
        } else if(chronicleConfig instanceof ChronicleConfig) {
            config.setDefaultChronicleConfig((ChronicleConfig) chronicleConfig);
        } else {
            throw new IllegalArgumentException("First argument is neither a VanillaChronicleConfig nor a ChronicleConfig");
        }
        return this;
    }

    public EventStoreFactory<T> setCycleFormat(String cycleFormat) {
        config.setCycleFormat(cycleFormat);
        return this;
    }

    public EventStoreFactory<T> setCycleLength(int cycleLength) {
        config.setCycleLength(cycleLength);
        return this;
    }

    public EventStoreFactory<T> setEntriesPerCycle(long entriesPerCycle) {
        config.setEntriesPerCycle(entriesPerCycle);
        return this;
    }

    public EventStoreFactory<T> setCycling(boolean cycling) {
        config.setCycling(cycling);
        return this;
    }

    public EventStoreFactory<T> withSerializers(Map<Class<? extends T>, Function<? extends T, byte[]>> serializers) {
        config.setSerializers(serializers);
        return this;
    }

    public EventStoreFactory<T> andDeserializers(Map<Class<? extends T>, Function<byte[], ? extends T>> deserializers) {
        config.setDeserializers(deserializers);
        return this;
    }

    public EventStoreFactory<T> setDataBlockSize(int dataBlockSize) {
        config.setDataBlockSize(dataBlockSize);
        return this;
    }

    public EventStoreFactory<T> havingMonotonicEventOrder(boolean monotonic) {
        config.setMonotonic(monotonic);
        return this;
    }

    public EventStore<T> build() throws IllegalArgumentException, IOException, ClassNotFoundException {
        if (config.isValid()) {
            try {

                if (config.isCycling()) {
                    // FIXME remove this after fixing vanilla issues
                    throw new UnsupportedOperationException("Cycling event stores are not supported yet");
                }
            ChronicleAdapter chronicle = config.isCycling() ?
                    new VanillaChronicleAdapterImpl(config.chronicleBasePath(), config.vanillaChronicleConfig()) :
                    new IndexedChronicleAdapterImpl(config.chronicleBasePath(), config.defaultChronicleConfig());
            return new ChronicleBasedEventStoreImpl<T>(chronicle, config);
            } catch (IOException e) {
                throw new FileNotFoundException("Can't create event store with base path " + config.chronicleBasePath());
            }
        } else {
            return null;
        }

    }

}
