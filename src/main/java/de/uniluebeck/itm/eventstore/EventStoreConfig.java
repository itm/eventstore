package de.uniluebeck.itm.eventstore;

import com.google.common.base.Function;
import de.uniluebeck.itm.eventstore.adapter.ChronicleConfigAdapter;
import de.uniluebeck.itm.eventstore.adapter.ChronicleConfigAdapterImpl;
import de.uniluebeck.itm.eventstore.adapter.VanillaChronicleConfigAdapterImpl;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.VanillaChronicleConfig;

import java.util.ArrayList;
import java.util.Map;

class EventStoreConfig<T> {

    private String chronicleBasePath;
    private boolean readOnly;
    private boolean monotonic;
    private boolean cycling;
    private String cycleFormat;
    private int cycleLength;
    private Map<Class<? extends T>, Function<? extends T, byte[]>> serializers;
    private Map<Class<? extends T>, Function<byte[], ? extends T>> deserializers;
    private int dataBlockSize;

    public EventStoreConfig() {
        readOnly = false;
        dataBlockSize = ChronicleConfig.SMALL.dataBlockSize();
        cycleFormat = VanillaChronicleConfig.DEFAULT.cycleFormat();
        cycleLength = VanillaChronicleConfig.DEFAULT.cycleLength();
    }

    public void setChronicleBasePath(String chronicleBasePath) {
        this.chronicleBasePath = chronicleBasePath;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void setSerializers(Map<Class<? extends T>, Function<? extends T, byte[]>> serializers) {
        this.serializers = serializers;
    }

    public void setDeserializers(Map<Class<? extends T>, Function<byte[], ? extends T>> deserializers) {
        this.deserializers = deserializers;
    }

    public void setDataBlockSize(int dataBlockSize) {
        this.dataBlockSize = dataBlockSize;
    }

    public void setMonotonic(boolean monotonic) {
        this.monotonic = monotonic;
    }


    public String cycleFormat() {
        return cycleFormat;
    }

    public void setCycleFormat(String cycleFormat) {
        cycling = true;
        this.cycleFormat = cycleFormat;
    }

    public int cycleLength() {
        return cycleLength;
    }

    public void setCycleLength(int cycleLength) {
        cycling = true;
        this.cycleLength = cycleLength;
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
        return dataBlockSize;
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
        return VanillaChronicleConfig.DEFAULT.clone().cycleFormat(cycleFormat).cycleLength(cycleLength).dataBlockSize(dataBlockSize);
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
        return ChronicleConfig.SMALL.clone().dataBlockSize(dataBlockSize);
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


}
