package timely;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.commons.configuration.BaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

public class StandaloneServer extends Server {

    private static final Logger LOG = LoggerFactory.getLogger(StandaloneServer.class);

    private static MiniAccumuloCluster mac = null;
    private static boolean isMacStarted = false;

    public StandaloneServer() {
        super();
    }

    private static File tempDir;

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    public void setup() {
        super.setup();
        startMiniAccumulo(config);
    }

    protected static void startMiniAccumulo(Configuration config) {
        if (!isMacStarted) {
            try {
                tempDir = Files.createTempDirectory("timely_standalone_mac_temp").toFile();
                tempDir.deleteOnExit();
                LOG.info("Starting MiniAccumuloCluster in directory: {}", tempDir);
            } catch (IOException e) {
                System.err.println("Unable to create temp directory for mini accumulo cluster");
                throw new RuntimeException(e);
            }

            File accumuloDir = new File(tempDir, "accumulo");
            MiniAccumuloConfig macConfig = new MiniAccumuloConfig(accumuloDir, config.getAccumulo().getPassword());
            macConfig.setInstanceName(config.getAccumulo().getInstanceName());
            macConfig.setZooKeeperPort(9804);
            macConfig.setNumTservers(1);
            macConfig.setMemory(ServerType.TABLET_SERVER, 1, MemoryUnit.GIGABYTE);
            try {
                mac = new MiniAccumuloCluster(macConfig);
                LOG.info("Starting MiniAccumuloCluster");
                mac.start();
                LOG.info("MiniAccumuloCluster started.");
                String instanceName = mac.getInstanceName();
                LOG.info("MiniAccumuloCluster instance name: {}", instanceName);

            } catch (IOException | InterruptedException e) {
                LOG.error("Error starting MiniAccumuloCluster: {}", e.getMessage());
                System.err.println("Error starting MiniAccumuloCluster: " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        isMacStarted = true;

        try {
            // reset to known state
            final BaseConfiguration apacheConf = new BaseConfiguration();
            Configuration.Accumulo accumuloConf = config.getAccumulo();
            apacheConf.setProperty("instance.name", accumuloConf.getInstanceName());
            apacheConf.setProperty("instance.zookeeper.host", accumuloConf.getZookeepers());
            final ClientConfiguration aconf = new ClientConfiguration(Collections.singletonList(apacheConf));
            Instance instance = new ZooKeeperInstance(aconf);
            Connector conn = instance.getConnector(config.getAccumulo().getUsername(), config.getAccumulo()
                    .getPassword());
            SecurityOperations sops = conn.securityOperations();
            Authorizations rootAuths = new Authorizations("A", "B", "C", "D", "E", "F", "G", "H", "I");
            sops.changeUserAuthorizations(config.getAccumulo().getUsername(), rootAuths);
            LOG.info("Configured root user auths for standalone.");
            conn.tableOperations().list().forEach(t -> {
                if (t.startsWith("timely")) {
                    try {
                        LOG.debug("Deleting table: {}", t);
                        conn.tableOperations().delete(t);
                    } catch (AccumuloException e) {
                        LOG.error("Accumulo Exception: {}", e.getMessage());
                    } catch (AccumuloSecurityException e) {
                        LOG.error("AccumuloSecurityException: {}", e.getMessage());
                    } catch (TableNotFoundException e) {
                        LOG.error("TableNotFound Exception: {}", e.getMessage());
                    }
                }
            });
        } catch (AccumuloException | AccumuloSecurityException e) {
            System.err.println("Error configuring root user");
            throw new RuntimeException(e);
        }
        LOG.info("Mini Accumulo Cluster setup complete.");
    }

    @Override
    public void run() throws Exception {
        super.run();
    }
}
