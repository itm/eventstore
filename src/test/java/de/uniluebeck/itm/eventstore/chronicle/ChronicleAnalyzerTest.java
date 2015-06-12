package de.uniluebeck.itm.eventstore.chronicle;


import de.uniluebeck.itm.eventstore.adapter.ChronicleAdapter;
import de.uniluebeck.itm.eventstore.adapter.IndexedChronicleAdapterImpl;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

public class ChronicleAnalyzerTest {

	private ChronicleAnalyzer analyzer;
	private ChronicleAdapter chronicle;

	@Before
	public void setUp() throws Exception {

		String basePath = System.getProperty("java.io.tmpdir") + "/IndexedChronicleAnalyzerTest";
		ChronicleTools.deleteOnExit(basePath);
		chronicle = new IndexedChronicleAdapterImpl(basePath, ChronicleConfig.DEFAULT.clone());
		analyzer = new ChronicleAnalyzer(chronicle);
	}

	@Test
	public void testActualSizeOfEmptyChronicle() throws IOException {
		assertEquals(0, analyzer.actualPayloadByteSize());
	}

	@Test
	public void testActualSizeAfterEntry() throws IOException {

		byte[] bytes = "Hello".getBytes();


		ExcerptAppender appender = chronicle.createAppender();
		appender.startExcerpt();
		appender.write(bytes);
		appender.finish();
		assertEquals(bytes.length, analyzer.actualPayloadByteSize());

		appender.startExcerpt();
		appender.write(bytes);
		appender.finish();
		assertEquals(2 * bytes.length, analyzer.actualPayloadByteSize());

		ExcerptAppender appender3 = chronicle.createAppender();
		appender3.startExcerpt();
		appender3.write(bytes);
		appender3.finish();
		assertEquals(3 * bytes.length, analyzer.actualPayloadByteSize());

		ExcerptAppender appender1 = chronicle.createAppender();
		appender1.startExcerpt(300000);
		appender1.write(new byte[200000]);
		appender1.write(new byte[100000]);
		appender1.finish();
		assertEquals(3 * bytes.length + 300000, analyzer.actualPayloadByteSize());

	}

	@Test
	public void testActualByteSizeAfterManyEntries() throws IOException {
		ExcerptAppender appender = chronicle.createAppender();
		long expected = 0;
		for (int i = 0; i < 1000; i++) {
			appender.startExcerpt(201);
			appender.write(new byte[200]);
			appender.finish();
			expected += 200;
			assertEquals(expected, analyzer.actualPayloadByteSize());

		}
	}

	@After
	public void tearDown() throws IOException {
		if (chronicle != null) {
			chronicle.close();
			chronicle = null;
		}
	}
}
