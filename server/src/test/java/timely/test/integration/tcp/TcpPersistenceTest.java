package timely.test.integration.tcp;

import com.google.inject.Inject;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.TestServer;
import timely.test.integration.MacITBase;

import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static timely.test.TestConfiguration.WAIT_SECONDS;
import static timely.test.integration.tcp.TimelyTcpIT.TEST_TIME;

/**
 *
 */
public class TcpPersistenceTest extends MacITBase {

    static final Logger LOG = LoggerFactory.getLogger(TcpPersistenceTest.class);

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

    @Override
    public void tearDownServer() throws Exception {
        // shutdown is explicitly done elsewhere
    }

    @Test
    public void testPersistence() throws Exception {
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                + " 1.0 tag3=value3 tag4=value4", "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
        sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
        testServer.shutdown();

        assertTrue(connector.namespaceOperations().exists("timely"));
        assertTrue(connector.tableOperations().exists("timely.metrics"));
        assertTrue(connector.tableOperations().exists("timely.meta"));

        int count = 0;
        for (final Map.Entry<Key, Value> entry : connector.createScanner("timely.metrics", Authorizations.EMPTY)) {
            LOG.info("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
        count = 0;
        for (final Map.Entry<Key, Value> entry : connector.createScanner("timely.meta", Authorizations.EMPTY)) {
            LOG.info("Meta entry: " + entry);
            count++;
        }
        assertEquals(10, count);
        // count w/out versioning iterator to make sure that the optimization
        // for writing is working
        connector.tableOperations().removeIterator("timely.meta", "vers", EnumSet.of(IteratorUtil.IteratorScope.scan));
        // wait for zookeeper propagation
        sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
        count = 0;
        for (final Map.Entry<Key, Value> entry : connector.createScanner("timely.meta", Authorizations.EMPTY)) {
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
        Thread.sleep(WAIT_SECONDS * 1000);
        testServer.shutdown();

        assertTrue(connector.namespaceOperations().exists("timely"));
        assertTrue(connector.tableOperations().exists("timely.metrics"));
        assertTrue(connector.tableOperations().exists("timely.meta"));

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
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write(format.toString());
            writer.flush();
        }
    }

}
