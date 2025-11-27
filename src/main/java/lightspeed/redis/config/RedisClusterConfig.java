package lightspeed.redis.config;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Configuration class for Redis Cluster connection.
 * Uses Lettuce client for async/reactive Redis operations with cluster support.
 */
public class RedisClusterConfig {

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterConfig.class);

    /**
     * Create Redis Cluster client with multiple seed nodes.
     * Lettuce will automatically discover all cluster nodes.
     *
     * @return configured RedisClusterClient
     */
    public static RedisClusterClient createClient() {
        List<RedisURI> nodes = Arrays.asList(
                RedisURI.create("redis://localhost:7000"),
                RedisURI.create("redis://localhost:7001"),
                RedisURI.create("redis://localhost:7002")
        );

        logger.info("Creating Redis Cluster client with seed nodes: {}", nodes);

        return RedisClusterClient.create(nodes);
    }

    /**
     * Create connection to Redis Cluster.
     * Connection is thread-safe and can be shared across application.
     *
     * @param client Redis cluster client
     * @return stateful cluster connection
     */
    public static StatefulRedisClusterConnection<String, String> createConnection(RedisClusterClient client) {
        logger.info("Establishing connection to Redis Cluster...");

        StatefulRedisClusterConnection<String, String> connection = client.connect();

        logger.info("Successfully connected to Redis Cluster");

        return connection;
    }

    /**
     * Cleanup resources.
     *
     * @param connection cluster connection
     * @param client cluster client
     */
    public static void cleanup(StatefulRedisClusterConnection<String, String> connection, RedisClusterClient client) {
        logger.info("Cleaning up Redis resources...");

        if (connection != null) {
            connection.close();
        }

        if (client != null) {
            client.shutdown();
        }

        logger.info("Redis resources cleaned up successfully");
    }
}
