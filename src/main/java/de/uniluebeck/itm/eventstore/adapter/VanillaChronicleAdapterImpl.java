package de.uniluebeck.itm.eventstore.adapter;


import net.openhft.chronicle.*;

import java.io.IOException;

public class VanillaChronicleAdapterImpl implements ChronicleAdapter {

    private final VanillaChronicle chronicle;
    private final VanillaChronicleConfigAdapterImpl config;


    public VanillaChronicleAdapterImpl(String basePath, VanillaChronicleConfig config) {
        this.config = new VanillaChronicleConfigAdapterImpl(config);
        this.chronicle = new VanillaChronicle(basePath, config);
    }
    @Override
    public void clear() {
        chronicle.clear();
    }

    @Override
    public void close() throws IOException{
        chronicle.close();
    }

    @Override
    public ChronicleConfigAdapter config() {
        return config;
    }

    @Override
    public ExcerptAppender createAppender() throws IOException{
        return chronicle.createAppender();
    }

    @Override
    public Excerpt createExcerpt() throws IOException{
        return chronicle.createExcerpt();
    }

    @Override
    public ExcerptTailer createTailer() throws IOException{
        return chronicle.createTailer();
    }

    @Override
    public long lastWrittenIndex() {
        return chronicle.lastWrittenIndex();
    }

    @Override
    public long size() {
        return chronicle.size();
    }

    @Override
    public String name() {
        return chronicle.name();
    }
}
