package timely.test.integration;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.TestServer;
import timely.guice.ConnectorProvider;
import timely.guice.TimelyModule;
import timely.guice.TimelyServerProvider;
import timely.test.TestConfiguration;
import timely.validator.TimelyServer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Base class for integration tests using mini accumulo cluster.
 */
public abstract class MacITBase {

    private static final Logger LOG = LoggerFactory.getLogger(MacITBase.class);

    private static final File tempDir;
    protected static Configuration conf;

    static {
        try {
            tempDir = Files.createTempDirectory("mac_temp").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create temp directory for mini accumulo cluster");
        }
        tempDir.deleteOnExit();
    }

    @Inject
    protected ConnectorProvider connectorProvider;
    protected Connector connector;

    @Inject
    protected TimelyServerProvider serverProvider;
    protected TimelyServer server;

    static Injector injector;


    @BeforeClass
    public static void setupMiniAccumulo() throws Exception {
        conf = TestConfiguration.createMinimalConfigurationForTest();
        conf.getSecurity().getSsl().setUseOpenssl(false);
        conf.getSecurity().getSsl().setUseGeneratedKeypair(true);
    }

    @Before
    public void setup() throws Exception {
        setupSSL();
        injector = Guice.createInjector(new TimelyModule(conf));
        injector.injectMembers(this);
        server = serverProvider.get();
        server.setup();

        connector = connectorProvider.get();

        server.run();
    }

    protected TimelyServer getRunningServer() throws Exception {
/*        injector = Guice.createInjector(new TimelyModule(conf));
        setupSSL();
        TimelyServer s = new StandaloneServer();
        injector.injectMembers(s);
        injector.injectMembers(this);
        s.setup();
        s.run();
        clearTablesResetConf();
        */
        clearTablesResetConf();
        return server;
    }

    protected TestServer getRunningTestServer() throws Exception {
        injector = Guice.createInjector(new TimelyModule(conf));
        injector.injectMembers(this);
        setupSSL();
        TestServer ts = new TestServer();
        injector.injectMembers(ts);
        ts.setup();
        ts.run();
        clearTablesResetConf();
        return ts;
    }

    public abstract void setupSSL() throws Exception;


    private void clearTablesResetConf() throws Exception {

        connector.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    connector.tableOperations().delete(t);
                } catch (AccumuloException e) {
                    e.printStackTrace();
                } catch (AccumuloSecurityException e) {
                    e.printStackTrace();
                } catch (TableNotFoundException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
