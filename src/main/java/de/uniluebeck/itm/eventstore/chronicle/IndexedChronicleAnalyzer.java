package de.uniluebeck.itm.eventstore.chronicle;


import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;

import java.io.IOException;

public class IndexedChronicleAnalyzer {

    private final IndexedChronicle chronicle;

    public IndexedChronicleAnalyzer(final IndexedChronicle chronicle) throws IOException {
        this.chronicle = chronicle;
    }

    /**
     * Method for getting the total payload size of all excerpts in the analyzers chronicle
     * <p/>
     * *Warning:* This method is very slow when working on large chronicles
     * because it has to read every single excerpt for counting bytes.
     *
     * @return the actual content size
     * @throws IOException if an I/O error occurs while reading from the chronicle
     */
    public long actualPayloadByteSize() throws IOException {
        ExcerptTailer reader = chronicle.createTailer();
        long size = 0;
        while (reader.nextIndex()) {
            while (reader.read() != -1) {
                size++;
            }
        }
        return size;
    }
}
