package timely;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.google.inject.Inject;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.validator.TimelyServer;

public class StandaloneServer implements TimelyServer {

    private static final Logger LOG = LoggerFactory.getLogger(StandaloneServer.class);

    private static MiniAccumuloCluster mac = null;

    @Inject
    private Configuration config;

    private final Server server;

    public StandaloneServer() {
        server = new Server();
    }

    @Override
    public void shutdown() {
        try {
            mac.stop();
            LOG.info("MiniAccumuloCluster shutdown.");
        } catch (IOException | InterruptedException e) {
            System.err.println("Error stopping MiniAccumuloCluster");
            e.printStackTrace();
        }
        server.shutdown();
    }

    @Override
    public void setup() {
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("timely_standalone_mac_temp").toFile();
            tempDir.deleteOnExit();
            LOG.info("Starting MiniAccumuloCluster in directory: {}", tempDir);
        } catch (IOException e) {
            System.err.println("Unable to create temp directory for mini accumulo cluster");
            System.exit(1);
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
            System.err.println("Error starting MiniAccumuloCluster: " + e.getMessage());
            System.exit(1);
        }

        try {
            Connector conn = mac.getConnector(config.getAccumulo().getUsername(), config.getAccumulo().getPassword());
            SecurityOperations sops = conn.securityOperations();
            Authorizations rootAuths = new Authorizations("A", "B", "C", "D", "E", "F", "G", "H", "I");
            sops.changeUserAuthorizations(config.getAccumulo().getUsername(), rootAuths);
        } catch (AccumuloException | AccumuloSecurityException e) {
            System.err.println("Error configuring root user");
            System.exit(1);
        }

        server.setup();
    }

    @Override
    public void run() throws Exception {
        server.run();
    }
}
