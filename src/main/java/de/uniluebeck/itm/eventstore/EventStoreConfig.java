package de.uniluebeck.itm.eventstore;

import com.google.common.base.Function;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.VanillaChronicleConfig;

import java.util.ArrayList;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

class EventStoreConfig<T> {

    private String chronicleBasePath;
    private boolean readOnly;
    private boolean monotonic;
    private boolean cycling;
    private Map<Class<? extends T>, Function<? extends T, byte[]>> serializers = newHashMap();
    private Map<Class<? extends T>, Function<byte[], ? extends T>> deserializers = newHashMap();

    private VanillaChronicleConfig vanillaChronicleConfig;
    private ChronicleConfig defaultChronicleConfig;

    public EventStoreConfig() {

        vanillaChronicleConfig = VanillaChronicleConfig.DEFAULT.clone();
        defaultChronicleConfig = ChronicleConfig.SMALL.clone();
        readOnly = false;
        monotonic = true;
        cycling = false;
    }

    public void setChronicleBasePath(String chronicleBasePath) {
        this.chronicleBasePath = chronicleBasePath;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void setSerializer(Class<? extends T> clazz, Function<? extends T, byte[]> serializer) {
        this.serializers.put(clazz, serializer);
    }

    public void setDeserializer(Class<? extends T> clazz, Function<byte[], ? extends T> deserializer) {
        this.deserializers.put(clazz, deserializer);
    }

    public void setDataBlockSize(int dataBlockSize) {
        vanillaChronicleConfig.dataBlockSize(dataBlockSize);
        defaultChronicleConfig.dataBlockSize(dataBlockSize);
    }

    public void setMonotonic(boolean monotonic) {
        this.monotonic = monotonic;
    }


    public String cycleFormat() {
        return vanillaChronicleConfig.cycleFormat();
    }

    public void setCycleFormat(String cycleFormat) {
        cycling = true;
        vanillaChronicleConfig.cycleFormat(cycleFormat);
    }

    public int cycleLength() {
        return vanillaChronicleConfig.cycleLength();
    }

    public void setCycleLength(int cycleLength) {
        cycling = true;
        vanillaChronicleConfig.cycleLength(cycleLength);
    }

    public long entriesPerCycle() {
        return vanillaChronicleConfig.entriesPerCycle();
    }

    public void setEntriesPerCycle(long entriesPerCycle) {
        cycling = true;
        vanillaChronicleConfig.entriesPerCycle(entriesPerCycle);
    }

    public void setVanillaChronicleConfig(VanillaChronicleConfig vanillaChronicleConfig) {
        cycling = true;
        this.vanillaChronicleConfig = vanillaChronicleConfig;
    }

    public void setDefaultChronicleConfig(ChronicleConfig defaultChronicleConfig) {
        cycling = false;
        this.defaultChronicleConfig = defaultChronicleConfig;
    }

    public String chronicleBasePath() {
        return chronicleBasePath;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isCycling() {
        return cycling;
    }

    public boolean isMonotonic() {
        return monotonic;
    }

    public Map<Class<? extends T>, Function<? extends T, byte[]>> serializers() {
        return serializers;
    }

    public Map<Class<? extends T>, Function<byte[], ? extends T>> deserializers() {
        return deserializers;
    }

    public int dataBlockSize() {
        return isCycling() ? (int) vanillaChronicleConfig.dataBlockSize() : defaultChronicleConfig.dataBlockSize();
    }


    public VanillaChronicleConfig vanillaChronicleConfig() throws IllegalStateException {
        try {
            isValid();

            if (!isCycling()) {
                throw new IllegalArgumentException("Can't create vanilla config for non cycling event store");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Can't create chronicle config from an invalid configuration.", e);
        }
        return vanillaChronicleConfig;
    }

    public ChronicleConfig defaultChronicleConfig() throws IllegalStateException {
        try {
            isValid();

            if (isCycling()) {
                throw new IllegalArgumentException("Can't create chronicle config for cycling event store");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Can't create chronicle config from an invalid configuration.", e);
        }
        return defaultChronicleConfig;
    }

    public boolean isValid() throws IllegalArgumentException {
        ArrayList<String> messages = new ArrayList<String>();
        if (serializers == null) {
            messages.add("The serializer map isn't set");
        }
        if (deserializers == null) {
            messages.add("The deserializer map isn't set");
        }
        if (serializers != null && deserializers != null) {
            if (serializers.size() != deserializers.size()) {
                messages.add("The number of deserializers and serializers is different but must be equal");
            }
            if (serializers.size() > 256) {
                messages.add("THe number oder serializers cannot be bigger than 256");
            }
            if (deserializers.size() > 256) {
                messages.add("THe number oder deserializers cannot be bigger than 256");
            }
        }
        if (chronicleBasePath == null) {
            messages.add("The event stores base path wasn't set");
        }

        if (messages.size() == 0) {
            return true;
        }
        StringBuilder messageBuilder = new StringBuilder("The event store configuration is invalid:");
        for (String message : messages) {
            messageBuilder.append("\n\t- ");
            messageBuilder.append(message);
        }
        throw new IllegalArgumentException(messageBuilder.toString());
    }


    public void setCycling(boolean cycling) {
        this.cycling = cycling;
    }
}
