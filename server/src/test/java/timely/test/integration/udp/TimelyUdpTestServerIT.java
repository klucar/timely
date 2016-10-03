package timely.test.integration.udp;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.inject.Inject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.TestServer;
import timely.api.request.MetricRequest;
import timely.auth.AuthCache;
import timely.model.Metric;
import timely.model.Tag;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;
import timely.test.integration.MacITBase;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Integration tests for the operations available over the UDP transport
 */
@Category(IntegrationTest.class)
public class TimelyUdpTestServerIT extends MacITBase {

    private static final Logger LOG = LoggerFactory.getLogger(TimelyUdpTestServerIT.class);
    private static final Long TEST_TIME = System.currentTimeMillis();

    @Inject
    TestServer testServer;

    @Before
    public void setup() throws Exception {
        startTimelyServer();
    }

    @Override
    public void setupAndRunServer() throws Exception {
        testServer.setup();
        testServer.run();
    }

    @After
    public void tearDownServer() throws Exception {
        testServer.shutdown();
    }

    @Test
    public void testPut() throws Exception {
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 54325);
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), 54325);
        try (DatagramSocket sock = new DatagramSocket()) {
            // @formatter:off
            packet.setData(("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n").getBytes(UTF_8));
            sock.send(packet);
            while (1 != testServer.getUdpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(1, testServer.getUdpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, testServer.getUdpRequests().getResponses().get(0).getClass());
            final MetricRequest actual = (MetricRequest) testServer.getUdpRequests().getResponses().get(0);
            final MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);
            // @formatter:on
        }
    }

    @Test
    public void testPutMultiple() throws Exception {
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 54325);
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), 54325);
        // @formatter:off
        try (DatagramSocket sock = new DatagramSocket()) {
            packet.setData(("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n"
                          + "put sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4\n").getBytes(UTF_8));
            sock.send(packet);
            while (2 != testServer.getUdpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(2, testServer.getUdpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, testServer.getUdpRequests().getResponses().get(0).getClass());
            MetricRequest actual = (MetricRequest) testServer.getUdpRequests().getResponses().get(0);
            MetricRequest expected = new MetricRequest (
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(MetricRequest.class, testServer.getUdpRequests().getResponses().get(1).getClass());
            actual = (MetricRequest) testServer.getUdpRequests().getResponses().get(1);
            expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.idle")
                            .value(TEST_TIME + 1, 1.0D)
                            .tag(new Tag("tag3", "value3"))
                            .tag(new Tag("tag4", "value4"))
                            .build()
            );
            Assert.assertEquals(expected, actual);
            // @formatter:on
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

        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 54325);
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), 54325);
        try (DatagramSocket sock = new DatagramSocket()) {
            packet.setData(data);
            sock.send(packet);
            while (2 != testServer.getUdpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(2, testServer.getUdpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, testServer.getUdpRequests().getResponses().get(0).getClass());
            // @formatter:off
            MetricRequest actual = (MetricRequest) testServer.getUdpRequests().getResponses().get(0);
            MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(MetricRequest.class, testServer.getUdpRequests().getResponses().get(1).getClass());
            actual = (MetricRequest) testServer.getUdpRequests().getResponses().get(1);
            expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.idle")
                            .value(TEST_TIME + 1, 1.0D)
                            .tag(new Tag("tag3", "value3"))
                            .tag(new Tag("tag4", "value4"))
                            .build()
            );
            Assert.assertEquals(expected, actual);
            // @formatter:on

        }
    }

    @Test
    public void testPutInvalidTimestamp() throws Exception {

        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 54325);
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), 54325);
        try (DatagramSocket sock = new DatagramSocket();) {
            packet.setData(("put sys.cpu.user " + TEST_TIME + "Z" + " 1.0 tag1=value1 tag2=value2\n").getBytes(UTF_8));
            sock.send(packet);
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            Assert.assertEquals(0, testServer.getUdpRequests().getCount());
        }
    }

}
