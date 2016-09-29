package timely.test.integration.udp;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.auth.AuthCache;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;
import timely.test.integration.MacITBase;
import timely.validator.TimelyServer;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for the operations available over the UDP transport
 */
@Category(IntegrationTest.class)
public class TimelyUdpIT extends MacITBase {

    private static final Logger LOG = LoggerFactory.getLogger(TimelyUdpIT.class);
    private static final Long TEST_TIME = System.currentTimeMillis();

    private TimelyServer server;

    @Before
    public void setup() throws Exception {
        connector.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    connector.tableOperations().delete(t);
                } catch (Exception e) {
                }
            }
        });
        server = getRunningServer();
    }

    @After
    public void tearDown() throws Exception {
        AuthCache.resetSessionMaxAge();
        server.shutdown();
    }

    @Override
    public void setupSSL() throws Exception {

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
    public void testPersistence() throws Exception {
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                + " 1.0 tag3=value3 tag4=value4", "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

        // todo cleanup
        //final ZooKeeperInstance inst = new ZooKeeperInstance(mac.getClientConfig());
        //final Connector connector = inst.getConnector("root", new PasswordToken("secret".getBytes(UTF_8)));

        assertTrue(connector.namespaceOperations().exists("timely"));
        assertTrue(connector.tableOperations().exists("timely.metrics"));
        assertTrue(connector.tableOperations().exists("timely.meta"));
        int count = 0;
        for (final Entry<Key, Value> entry : connector.createScanner("timely.metrics", Authorizations.EMPTY)) {
            LOG.info("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
        count = 0;
        for (final Entry<Key, Value> entry : connector.createScanner("timely.meta", Authorizations.EMPTY)) {
            LOG.info("Meta entry: " + entry);
            count++;
        }
        assertEquals(10, count);
        // count w/out versioning iterator to make sure that the optimization
        // for writing is working
        connector.tableOperations().removeIterator("timely.meta", "vers", EnumSet.of(IteratorScope.scan));
        // wait for zookeeper propagation
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        count = 0;
        for (final Entry<Key, Value> entry : connector.createScanner("timely.meta", Authorizations.EMPTY)) {
            LOG.info("Meta no vers iter: " + entry);
            count++;
        }
        assertEquals(10, count);
    }

    @Test
    public void testPersistenceWithVisibility() throws Exception {
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                + " 1.0 tag3=value3 tag4=value4 viz=(a|b)", "sys.cpu.idle " + (TEST_TIME + 2)
                + " 1.0 tag3=value3 tag4=value4 viz=(c&b)");
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

        // todo cleanup
        //final ZooKeeperInstance inst = new ZooKeeperInstance(mac.getClientConfig());
        //final Connector connector = inst.getConnector("root", new PasswordToken("secret".getBytes(UTF_8)));
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("a", "b", "c"));

        int count = 0;
        for (final Map.Entry<Key, Value> entry : connector.createScanner("timely.metrics", Authorizations.EMPTY)) {
            LOG.info("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(2, count);
        count = 0;
        Authorizations auth1 = new Authorizations("a");
        for (final Map.Entry<Key, Value> entry : connector.createScanner("timely.metrics", auth1)) {
            LOG.info("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(4, count);
        count = 0;
        Authorizations auth2 = new Authorizations("b", "c");
        for (final Map.Entry<Key, Value> entry : connector.createScanner("timely.metrics", auth2)) {
            LOG.info("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
    }

    private void put(String... lines) throws Exception {
        StringBuffer format = new StringBuffer();
        for (String line : lines) {
            format.append("put ");
            format.append(line);
            format.append("\n");
        }
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 54325);
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), 54325);
        try (DatagramSocket sock = new DatagramSocket();) {
            packet.setData(format.toString().getBytes(UTF_8));
            sock.send(packet);
        }
    }

}
