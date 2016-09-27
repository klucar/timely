package timely.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.configuration.BaseConfiguration;
import timely.Configuration;
import timely.adapter.accumulo.TableHelper;
import timely.validator.TimelyServer;

import java.util.Collections;

/**
 *
 *
 */
public class TimelyModule extends AbstractModule {

    private final Configuration configuration;
    private Instance instance = null;
    private Connector connector = null;

    public TimelyModule(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected void configure() {
        bind(Configuration.class).toInstance(configuration);

        try {
            Class clazz = Class.forName(configuration.getServerClassName());
            bind(TimelyServer.class).toInstance((TimelyServer) clazz.newInstance());
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Provides
    Connector provideConnector() {
        if (null == instance) {
            final BaseConfiguration apacheConf = new BaseConfiguration();
            Configuration.Accumulo accumuloConf = configuration.getAccumulo();
            apacheConf.setProperty("instance.name", accumuloConf.getInstanceName());
            apacheConf.setProperty("instance.zookeeper.host", accumuloConf.getZookeepers());
            final ClientConfiguration aconf = new ClientConfiguration(Collections.singletonList(apacheConf));
            instance = new ZooKeeperInstance(aconf);
        }
        if (null == connector && null != instance) {
            try {
                connector = instance.getConnector(configuration.getAccumulo().getUsername(), new PasswordToken(
                        configuration.getAccumulo().getPassword()));
            } catch (AccumuloException e) {
                // todo handle
                e.printStackTrace();
            } catch (AccumuloSecurityException e) {
                // todo handle
                e.printStackTrace();
            }
        }
        return connector;
    }

    @Provides
    TableHelper provideTableHelper() {
        return new TableHelper();
    }
}
