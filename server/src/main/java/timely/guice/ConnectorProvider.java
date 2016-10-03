package timely.guice;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.configuration.BaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;

import java.util.Collections;

/**
 * Created by jlkluca on 9/29/16.
 */
public class ConnectorProvider implements Provider<Connector> {

    static final Logger LOG = LoggerFactory.getLogger(ConnectorProvider.class);

    private Instance instance = null;
    private Connector connector = null;

    @Inject
    private Configuration configuration;

    public ConnectorProvider() {
    }

    @Override
    public Connector get() {
        if (null == instance) {
            final BaseConfiguration apacheConf = new BaseConfiguration();
            Configuration.Accumulo accumuloConf = configuration.getAccumulo();
            apacheConf.setProperty("instance.name", accumuloConf.getInstanceName());
            apacheConf.setProperty("instance.zookeeper.host", accumuloConf.getZookeepers());
            final ClientConfiguration aconf = new ClientConfiguration(Collections.singletonList(apacheConf));
            instance = new ZooKeeperInstance(aconf);
            LOG.debug("Created ZooKeeperInstance for instance: {} at zk: {}", accumuloConf.getInstanceName(),
                    accumuloConf.getZookeepers());
        }
        if (null == connector && null != instance) {
            try {
                connector = instance.getConnector(configuration.getAccumulo().getUsername(), new PasswordToken(
                        configuration.getAccumulo().getPassword()));
                LOG.debug("Created Accumulo Connector for user: {}", configuration.getAccumulo().getUsername());
            } catch (AccumuloException e) {
                // todo handle
                LOG.error("AccumuloException creating connector to accumulo instance: {}", e.getMessage());
                e.printStackTrace();
            } catch (AccumuloSecurityException e) {
                // todo handle
                LOG.error("AccumuloSecurityException error creating connector to accumulo instance: {}", e.getMessage());
                e.printStackTrace();
            }
        }
        return connector;

    }

}
