package timely.test.integration;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.accumulo.core.client.Connector;
import org.junit.After;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.TestModule;
import timely.cache.AuthCache;
import timely.guice.provider.ConnectorProvider;
import timely.test.TestConfiguration;
import timely.validator.TimelyServer;

/**
 * Base class for integration tests using mini accumulo cluster.
 */
public abstract class MacITBase {

    private static final Logger LOG = LoggerFactory.getLogger(MacITBase.class);

    protected static Configuration conf;

    @Inject
    protected ConnectorProvider connectorProvider;
    protected Connector connector;

    @Inject
    protected TimelyServer server;

    Injector injector;

    @BeforeClass
    public static void setupMiniAccumulo() throws Exception {
        conf = TestConfiguration.createMinimalConfigurationForTest();
        conf.getSecurity().getSsl().setUseOpenssl(false);
        conf.getSecurity().getSsl().setUseGeneratedKeypair(true);
    }

    @After
    public void tearDownServer() throws Exception {
        server.shutdown();
    }

    // timely server must be setup and run in the tests because tests may want
    // to change conf
    public void startTimelyServer() throws Exception {
        injector = Guice.createInjector(new TestModule(conf));
        injector.injectMembers(this);
        // AuthCache.resetSessionMaxAge();
        setupAndRunServer();
        connector = connectorProvider.get();
    }

    public void setupAndRunServer() throws Exception {
        server.setup();
        server.run();
    }

    /*
     * protected TestServer startTestServer() throws Exception { injector =
     * Guice.createInjector(new TestModule(conf)); injector.injectMembers(this);
     * server.setup(); connector = connectorProvider.get(); server.run(); return
     * (TestServer) server; }
     */

}
