package de.uniluebeck.itm.eventstore.adapter;


import net.openhft.chronicle.Chronicle;

public interface ChronicleAdapter extends Chronicle {

    ChronicleConfigAdapter config();


}
