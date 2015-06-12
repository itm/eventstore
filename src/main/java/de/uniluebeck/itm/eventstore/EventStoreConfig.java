package de.uniluebeck.itm.eventstore;

import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.VanillaChronicleConfig;

import java.util.ArrayList;
import java.util.function.Function;

class EventStoreConfig<T> {

	private String chronicleBasePath;

	private boolean readOnly;

	private boolean monotonic;

	private boolean cycling;

	private VanillaChronicleConfig vanillaChronicleConfig;

	private ChronicleConfig defaultChronicleConfig;

	private Function<T, byte[]> serializer;

	private Function<byte[], T> deserializer;

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

	public void setDataBlockSize(int dataBlockSize) {
		vanillaChronicleConfig.dataBlockSize(dataBlockSize);
		defaultChronicleConfig.dataBlockSize(dataBlockSize);
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

	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	public boolean isCycling() {
		return cycling;
	}

	public void setCycling(boolean cycling) {
		this.cycling = cycling;
	}

	public boolean isMonotonic() {
		return monotonic;
	}

	public void setMonotonic(boolean monotonic) {
		this.monotonic = monotonic;
	}

	public Function<T, byte[]> getSerializer() {
		return serializer;
	}

	public void setSerializer(Function<T, byte[]> serializer) {
		this.serializer = serializer;
	}

	public Function<byte[], T> getDeserializer() {
		return deserializer;
	}

	public void setDeserializer(Function<byte[], T> deserializer) {
		this.deserializer = deserializer;
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
		if (serializer == null) {
			messages.add("The serializer isn't set");
		}
		if (deserializer == null) {
			messages.add("The deserializer isn't set");
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
