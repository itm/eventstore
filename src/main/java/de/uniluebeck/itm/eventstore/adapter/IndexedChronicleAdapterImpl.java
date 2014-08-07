package de.uniluebeck.itm.eventstore.adapter;


import net.openhft.chronicle.*;

import java.io.IOException;

public class IndexedChronicleAdapterImpl implements ChronicleAdapter {

    private final IndexedChronicle chronicle;
    private final ChronicleConfigAdapterImpl config;



    public IndexedChronicleAdapterImpl(String basePath, ChronicleConfig config) throws IOException{

        this.config = new ChronicleConfigAdapterImpl(config);
        this.chronicle = new IndexedChronicle(basePath, config);
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
