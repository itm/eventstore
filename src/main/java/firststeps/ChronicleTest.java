package firststeps;

import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ChronicleTest {

	public static final String CHRONICLE_PREFIX =
			"/Volumes/BigBrain/Arbeit/ITM/testbed-event-store/chronicle-test/chronicles/";

	public static final String LOG_PREFIX = "/Volumes/BigBrain/Arbeit/ITM/testbed-event-store/chronicle-test/logs/";

	public static void main(String[] args) {


		List<TestConfiguration> configurations = buildConfigurations();
		FileWriter writer = null;
		try {
			writer = new FileWriter(new File(LOG_PREFIX + "test.data"));
			writer.append("data size\titerations\tsleep time\twrite time\tsequential read time\trandom read time\n");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		for (TestConfiguration config : configurations) {
			try {
				final IndexedChronicle chronicle = new IndexedChronicle(CHRONICLE_PREFIX + config.toString());
				long totalWriteTime = writeTest(chronicle, config);
				long totalSequentialReadTime = sequentialReadTest(chronicle, config);
				long totalRandomReadTime = randomReadTest(chronicle, config);
				writer.append(config.getDataSize() + "\t" + config.getIterations() + "\t" + config
						.getSleepTimeInMs() + "\t" + totalWriteTime + "\t" + totalSequentialReadTime + "\t" + totalRandomReadTime + "\n"
				);
				chronicle.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	private static List<TestConfiguration> buildConfigurations() {
		List<TestConfiguration> configurations = new ArrayList<TestConfiguration>();
		/*configurations.add(new TestConfiguration(15360, 100000, 1));        // 15 kb, 1000 events/second
        configurations.add(new TestConfiguration(157286, 10000, 10));       // 0.15 mb, 100 events/second
        configurations.add(new TestConfiguration(1572864, 1000, 100));       // 1.5 mb, 10 events/second
        configurations.add(new TestConfiguration(15728640, 100, 1000));    // 15 mb, 1 event/second*/
		configurations.add(new TestConfiguration(15360, 100000, 1));        // 15 kb, 1000 events/second
		configurations.add(new TestConfiguration(157286, 100000, 10));       // 0.15 mb, 100 events/second
		configurations.add(new TestConfiguration(1572864, 100000, 100));       // 1.5 mb, 10 events/second
		configurations.add(new TestConfiguration(15728640, 100000, 1000));    // 15 mb, 1 event/second
		return configurations;
	}

	private static long writeTest(final IndexedChronicle chronicle, TestConfiguration config) {
		long result = 0;
		System.out.println("WRITE TEST - " + config);
		FileWriter writer = null;
		try {
			writer = new FileWriter(new File(LOG_PREFIX + "write-" + config.toString()));
			writer.append("iteration\twrite time\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (int iteration = 0; iteration < config.getIterations(); iteration++) {
			if (iteration % 1000 == 0) {
				System.out.println("\t starting iteration " + iteration + "/" + config.getIterations());
			}
			byte[] data = generateTestData(config.getDataSize());
			long startTime = System.nanoTime();
			try {
				final ExcerptAppender appender;
				appender = chronicle.createAppender();
				appender.startExcerpt(config.getDataSize() + 8);
				appender.write(data);
				appender.finish();
				long diff = (System.nanoTime() - startTime);
				result += diff;
				writer.write(iteration + "\t" + diff + "\n");
				writer.flush();
				if (iteration % 1000 == 0) {
					System.out.println("\t\t-> " + diff + " ns");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(config.getSleepTimeInMs());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return result;
	}

	private static long sequentialReadTest(final IndexedChronicle chronicle, TestConfiguration config) {
		long result = 0;
		long elementCounter = 0;
		System.out.println("SEQUENTIAL READ TEST - " + config);
		long startTime = System.nanoTime();
		try {
			ExcerptTailer reader = chronicle.createTailer();
			while (reader.nextIndex()) {
				elementCounter++;
				reader.read();
			}
			reader.finish();
		} catch (IOException e) {
			e.printStackTrace();
		}
		result = System.nanoTime() - startTime;
		System.out.println("\tread " + elementCounter + " entries");
		return result;
	}

	private static long randomReadTest(final IndexedChronicle chronicle, TestConfiguration config) {
		long result = 0;
		System.out.println("RANDOM READ TEST - " + config);
		Random random = new Random(System.currentTimeMillis());
		FileWriter writer = null;
		try {
			writer = new FileWriter(new File(LOG_PREFIX + "random-" + config.toString()));
			writer.append("iteration\tread time\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (int iteration = 0; iteration < config.getIterations(); iteration++) {
			long startTime = System.nanoTime();
			ExcerptTailer reader = null;
			long diff = (System.nanoTime() - startTime);
			result += diff;
			try {
				reader = chronicle.createTailer();
				reader.index(random.nextInt(config.getIterations()));
				reader.read();
				writer.write(iteration + "\t" + diff + "\n");
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("\t finished iteration " + iteration + "/" + config.getIterations() + " - " + diff);
		}

		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;

	}

	private static byte[] generateTestData(int size) {
		byte[] result = new byte[size];
		new Random(System.currentTimeMillis()).nextBytes(result);
		return result;
	}

}
