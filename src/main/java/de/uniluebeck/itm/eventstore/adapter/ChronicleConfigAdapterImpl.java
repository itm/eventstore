package de.uniluebeck.itm.eventstore.adapter;

import net.openhft.chronicle.ChronicleConfig;

public class ChronicleConfigAdapterImpl implements ChronicleConfigAdapter {

    private ChronicleConfig config;

   public  ChronicleConfigAdapterImpl(ChronicleConfig config) {

        this.config = config;
    }

    @Override
    public int dataBlockSize() {
        return config.dataBlockSize();
    }

    @Override
    public ChronicleConfigAdapter dataBlockSize(int dataBlockSize) {
        config = config.dataBlockSize(dataBlockSize);
        return this;
    }
}
