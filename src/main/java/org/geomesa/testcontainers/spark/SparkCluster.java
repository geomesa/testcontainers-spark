package org.geomesa.testcontainers.spark;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Spark cluster consisting of one master and one worker
 */
public class SparkCluster implements Startable {

    public static final DockerImageName DEFAULT_IMAGE =
            DockerImageName.parse("apache/spark")
                    .withTag(System.getProperty("spark.docker.tag", "3.5.7-scala2.12-java17-ubuntu"));

    private static final Logger logger = LoggerFactory.getLogger(SparkCluster.class);

    /**
     * The master container, accessible for customizing the container or mounting in additional files
     */
    public final SparkContainer master =
            new SparkContainer()
                    .withNetworkAliases("spark-master")
                    .withExposedPorts(7077)
                    .withEnv("SPARK_LOCAL_IP", "spark-master")
                    .withCommand("/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master")
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("spark-master")));

    /**
     * The worker container, accessible for customizing the container or mounting in additional files
     */
    public final SparkContainer worker =
            new SparkContainer()
                    .withNetworkAliases("spark-worker")
                    .withEnv("SPARK_WORKER_CORES", System.getProperty("spark.worker.cores", "2"))
                    .withEnv("SPARK_WORKER_MEMORY", System.getProperty("spark.worker.memory", "1g"))
                    .withCommand("/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077")
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("spark-worker")))
                    .withAccessToHost(true) // needed to call back to the driver running locally
                    .dependsOn(master);

    private SparkSession spark;

    /**
     * Create a cluster and mount any geomesa-*-spark-runtime jars that are available on the classpath
     */
    public SparkCluster() {
        this(findSparkRuntimeJars());
    }

    /**
     * Create a cluster and mount the given jars into distributed classpath
     *
     * @param distributedClasspathJars jars to mount
     */
    public SparkCluster(Collection<File> distributedClasspathJars) {
        for (File jarFile: distributedClasspathJars) {
            var hostPath = jarFile.getAbsolutePath();
            logger.info("Mounting jar: {}", hostPath);
            worker.withFileSystemBind(hostPath, "/opt/spark/jars/" + jarFile.getName(), BindMode.READ_ONLY);
        }
    }

    /**
     * Set the network used by the cluster
     *
     * @param network network
     * @return the cluster
     */
    public SparkCluster withNetwork(Network network) {
        master.setNetwork(network);
        worker.setNetwork(network);
        return this;
    }

    @Override
    public synchronized void start() {
        if (master.getNetwork() == null) {
            var network = Network.newNetwork();
            master.setNetwork(network);
            worker.setNetwork(network);
        }
        master.start();
        worker.start();
    }

    @Override
    public void stop() {
        if (spark != null) {
            spark.close();
        }
        worker.close();
        master.close();
    }

    /**
     * Gets a spark session connected to this cluster. Note that only a single spark session can be active at one
     * time within a given JVM.
     * <p>
     * Note it is an error to call this method when the cluster is not running.
     *
     * @return the spark session
     */
    public synchronized SparkSession getOrCreateSession() {
        if (spark == null) {
            if (!master.isRunning()) {
                throw new RuntimeException("Cluster is not running - did you call start()?");
            }

            var driverPort = getFreePort();
            var blockManagerPort = getFreePort();
            logger.debug("Driver port: {}", driverPort);
            logger.debug("BlockManager port: {}", blockManagerPort);
            Testcontainers.exposeHostPorts(driverPort, blockManagerPort);

            spark =
                    SparkSession.builder()
                            .appName("test")
                            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                            .config("spark.kryo.registrator", "org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator")
                            .config("spark.sql.crossJoin.enabled", "true")
                            .config("spark.ui.enabled", "false")
                            .config("spark.driver.bindAddress", "0.0.0.0")
                            .config("spark.driver.host", "host.testcontainers.internal") // so that the cluster can communicate back to the driver
                            .config("spark.driver.port", driverPort)
                            .config("spark.driver.blockManager.port", blockManagerPort)
                            .master(String.format("spark://%s:%s",master.getHost(), master.getMappedPort(7077)))
                            .getOrCreate();
        }
        return spark;
    }

    /**
     * Finds any geomesa-*-spark-runtime jars that are on the classpath
     *
     * @return list of uris to the jar files
     */
    private static Collection<File> findSparkRuntimeJars() {
        List<File> jars = new ArrayList<>();
        try {
            // load our spark-runtime jars into the worker classpath - this props file comes from the shaded geomesa-utils jar
            var urls = SparkCluster.class.getClassLoader().getResources("org/locationtech/geomesa/geomesa.properties");
            while (urls.hasMoreElements()) {
                var uri = urls.nextElement().toURI();
                if ("jar".equals(uri.getScheme()) &&
                        // patterns for: geomesa 6+ || earlier
                        (uri.toString().contains("-runtime.jar") || uri.toString().contains("spark-runtime"))) {
                    // uris look like: jar://file://foo.jar!/path/in/jar
                    var jar = uri.toString().substring(4).replaceAll("\\.jar!.*", ".jar");
                    jars.add(Paths.get(URI.create(jar)).toFile());
                }
            }
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
        return jars;
    }

    private static int getFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Unable to get free port", e);
        }
    }

    /**
     * Generic spark container
     */
    public static class SparkContainer extends GenericContainer<SparkContainer> {
        public SparkContainer() {
            super(DEFAULT_IMAGE);
        }
    }
}
