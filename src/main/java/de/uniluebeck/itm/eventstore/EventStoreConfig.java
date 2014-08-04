package de.uniluebeck.itm.eventstore;

import com.google.common.base.Function;
import net.openhft.chronicle.ChronicleConfig;

import java.util.ArrayList;
import java.util.Map;

class EventStoreConfig<T> {

    private String chronicleBasePath;
    private boolean readOnly;
    private boolean monotonic;
    private Map<Class<? extends T>, Function<? extends T, byte[]>> serializers;
    private Map<Class<? extends T>, Function<byte[], ? extends T>> deserializers;
    private int dataBlockSize;

    public EventStoreConfig() {
        readOnly = false;
        dataBlockSize = ChronicleConfig.SMALL.dataBlockSize();
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


    public String chronicleBasePath() {
        return chronicleBasePath;
    }

    public boolean isReadOnly() {
        return readOnly;
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
