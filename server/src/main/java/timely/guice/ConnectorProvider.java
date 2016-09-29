package timely.guice;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.configuration.BaseConfiguration;
import timely.Configuration;

import java.util.Collections;

/**
 * Created by jlkluca on 9/29/16.
 */
public class ConnectorProvider implements Provider<Connector> {

    private Instance instance = null;
    private Connector connector = null;

    @Inject
    private Configuration configuration;


    public ConnectorProvider(){}

    @Override
    public Connector get() {
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

}
