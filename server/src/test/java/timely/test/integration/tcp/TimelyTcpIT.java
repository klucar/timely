package timely.test.integration.tcp;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.inject.Inject;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.TestServer;
import timely.api.request.MetricRequest;
import timely.api.request.VersionRequest;
import timely.auth.AuthCache;
import timely.model.Metric;
import timely.model.Tag;
import timely.test.IntegrationTest;
import timely.test.integration.MacITBase;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static timely.test.TestConfiguration.WAIT_SECONDS;

/**
 * Integration tests for the operations available over the TCP transport
 */
@Category(IntegrationTest.class)
public class TimelyTcpIT extends MacITBase {

    static final Logger LOG = LoggerFactory.getLogger(TimelyTcpIT.class);
    static final Long TEST_TIME = System.currentTimeMillis();

    @Inject
    TestServer testServer;

    @Override
    public void setupAndRunServer() throws Exception {
        testServer.setup();
        testServer.run();
    }

    @Before
    public void setup() throws Exception {
        startTimelyServer();
    }

    @After
    public void tearDownServer() throws Exception {
        testServer.shutdown();
    }

    @Test
    public void testVersion() throws Exception {
        Socket sock = new Socket("127.0.0.1", 54321);
        PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);
        writer.write("version\n");
        writer.flush();
        while (1 != testServer.getTcpRequests().getCount()) {
            Thread.sleep(5);
        }
        Assert.assertEquals(1, testServer.getTcpRequests().getResponses().size());
        Assert.assertEquals(VersionRequest.class, testServer.getTcpRequests().getResponses().get(0).getClass());
        VersionRequest v = (VersionRequest) testServer.getTcpRequests().getResponses().get(0);
        Assert.assertEquals(VersionRequest.VERSION, v.getVersion());
    }

    @Test
    public void testPut() throws Exception {
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n");
            writer.flush();
            while (1 != testServer.getTcpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(1, testServer.getTcpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, testServer.getTcpRequests().getResponses().get(0).getClass());
            final MetricRequest actual = (MetricRequest) testServer.getTcpRequests().getResponses().get(0);
            // @formatter:off
            final MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            // @formatter on
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testPutMultiple() throws Exception {
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true)) {
            // @formatter:off
            writer.write("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n"
                       + "put sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4\n");
            writer.flush();
            while (2 != testServer.getTcpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(2, testServer.getTcpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, testServer.getTcpRequests().getResponses().get(0).getClass());
            MetricRequest actual = (MetricRequest) testServer.getTcpRequests().getResponses().get(0);
            MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(MetricRequest.class, testServer.getTcpRequests().getResponses().get(1).getClass());
            actual = (MetricRequest) testServer.getTcpRequests().getResponses().get(1);
            expected = new MetricRequest(
                    Metric.newBuilder()
                        .name("sys.cpu.idle")
                        .value(TEST_TIME + 1, 1.0D)
                        .tag(new Tag("tag3", "value3"))
                        .tag(new Tag("tag4", "value4"))
                        .build()
            );
            // @formatter:on
            Assert.assertEquals(expected, actual);

        }
    }

    private int createMetric(FlatBufferBuilder builder, String name, long timestamp, double value,
            Map<String, String> tags) {
        int n = builder.createString(name);
        int[] t = new int[tags.size()];
        int i = 0;
        for (Entry<String, String> e : tags.entrySet()) {
            t[i] = timely.api.flatbuffer.Tag.createTag(builder, builder.createString(e.getKey()),
                    builder.createString(e.getValue()));
            i++;
        }
        return timely.api.flatbuffer.Metric.createMetric(builder, n, timestamp, value,
                timely.api.flatbuffer.Metric.createTagsVector(builder, t));
    }

    @Test
    public void testPutMultipleBinary() throws Exception {

        FlatBufferBuilder builder = new FlatBufferBuilder(1);

        int[] metric = new int[2];
        Map<String, String> t = new HashMap<>();
        t.put("tag1", "value1");
        t.put("tag2", "value2");
        metric[0] = createMetric(builder, "sys.cpu.user", TEST_TIME, 1.0D, t);
        t = new HashMap<>();
        t.put("tag3", "value3");
        t.put("tag4", "value4");
        metric[1] = createMetric(builder, "sys.cpu.idle", TEST_TIME + 1, 1.0D, t);

        int metricVector = timely.api.flatbuffer.Metrics.createMetricsVector(builder, metric);

        timely.api.flatbuffer.Metrics.startMetrics(builder);
        timely.api.flatbuffer.Metrics.addMetrics(builder, metricVector);
        int metrics = timely.api.flatbuffer.Metrics.endMetrics(builder);
        timely.api.flatbuffer.Metrics.finishMetricsBuffer(builder, metrics);

        ByteBuffer binary = builder.dataBuffer();
        byte[] data = new byte[binary.remaining()];
        binary.get(data, 0, binary.remaining());
        LOG.debug("Sending {} bytes", data.length);

        try (Socket sock = new Socket("127.0.0.1", 54321);) {
            sock.getOutputStream().write(data);
            sock.getOutputStream().flush();
            while (2 != testServer.getTcpRequests().getCount()) {
                LOG.debug("Thread sleeping");
                Thread.sleep(5);
            }
            Assert.assertEquals(2, testServer.getTcpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, testServer.getTcpRequests().getResponses().get(0).getClass());
            // @formatter:off
            MetricRequest actual = (MetricRequest) testServer.getTcpRequests().getResponses().get(0);
            MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(MetricRequest.class, testServer.getTcpRequests().getResponses().get(1).getClass());
            actual = (MetricRequest) testServer.getTcpRequests().getResponses().get(1);
            expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.idle")
                            .value(TEST_TIME + 1, 1.0D)
                            .tag(new Tag("tag3", "value3"))
                            .tag(new Tag("tag4", "value4"))
                            .build()
            );
            // @formatter:on
            Assert.assertEquals(expected, actual);

        }
    }

    @Test
    public void testPutInvalidTimestamp() throws Exception {
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));) {
            writer.write("put sys.cpu.user " + TEST_TIME + "Z" + " 1.0 tag1=value1 tag2=value2\n");
            writer.flush();
            sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
            Assert.assertEquals(0, testServer.getTcpRequests().getCount());
        }
    }

}
