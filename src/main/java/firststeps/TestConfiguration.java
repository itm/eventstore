package firststeps;

public class TestConfiguration {

	private int dataSize;

	private int iterations;

	private long sleepTimeInMs;

	public TestConfiguration(int dataSize, int iterations, long sleepTimeInMs) {
		this.dataSize = dataSize;
		this.iterations = iterations;
		this.sleepTimeInMs = sleepTimeInMs;
	}

	public int getDataSize() {
		return dataSize;
	}

	public int getIterations() {
		return iterations;
	}

	public long getSleepTimeInMs() {
		return sleepTimeInMs;
	}

	@Override
	public String toString() {
		return "test-" + dataSize + "-" + iterations + "-" + sleepTimeInMs;
	}
}
