package de.uniluebeck.itm.eventstore.adapter;

import net.openhft.chronicle.VanillaChronicleConfig;

public class VanillaChronicleConfigAdapterImpl implements ChronicleConfigAdapter {

    private VanillaChronicleConfig config;

    public VanillaChronicleConfigAdapterImpl(VanillaChronicleConfig config) {

        this.config = config;
    }

    @Override
    public int dataBlockSize() {
        return (int) config.dataBlockSize();
    }

    @Override
    public ChronicleConfigAdapter dataBlockSize(int dataBlockSize) {
         config = config.dataBlockSize(dataBlockSize);
        return this;
    }
}
