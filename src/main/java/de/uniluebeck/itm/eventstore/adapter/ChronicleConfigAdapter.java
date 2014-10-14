package de.uniluebeck.itm.eventstore.adapter;


public interface ChronicleConfigAdapter {

    int dataBlockSize();
    ChronicleConfigAdapter dataBlockSize(int dataBlockSize);
}
