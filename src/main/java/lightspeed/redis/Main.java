package lightspeed.redis;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lightspeed.redis.config.RedisClusterConfig;
import lightspeed.redis.data.RedisHashMap;
import lightspeed.redis.data.RedisList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Demo application showing RedisHashMap usage.
 * <p>
 * Prerequisites:
 * 1. Start Redis Cluster: docker-compose up -d
 * 2. Wait for cluster initialization (check logs: docker-compose logs redis-cluster-init)
 * 3. Run this demo: mvn exec:java
 * <p>
 * For detailed tests, run: mvn test
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * Prerequisite: start Redis Cluster via {@code docker-compose -f infra/docker-compose.yml up -d}
     * before running the demo.
     */
    public static void main(String[] args) {
        logger.info("=== Redis Collections Demo ===\n(Ensure Redis Cluster is running: docker-compose -f infra/docker-compose.yml up -d)\n");

        // 1. Connect to Redis Cluster
        RedisClusterClient client = RedisClusterConfig.createClient();
        StatefulRedisClusterConnection<String, String> connection = RedisClusterConfig.createConnection(client);
        RedisClusterCommands<String, String> redis = connection.sync();

        try {
            // 2. Create RedisHashMap
            RedisHashMap userScores = new RedisHashMap(client, connection, "user_scores");

            // 3. Run demos
            demonstrateBasicUsage(userScores);
            demonstrateTTL(userScores);
            demonstrateClusterSharding(userScores, redis);
            demonstrateListUsage(connection);

            logger.info("\n=== Demo completed! Run 'mvn test' for detailed tests ===");

        } catch (Exception e) {
            logger.error("Error during demo", e);
        } finally {
            RedisClusterConfig.cleanup(connection, client);
        }
    }

    /**
     * Demonstrate basic Map operations as you'd use any java.util.Map.
     */
    private static void demonstrateBasicUsage(Map<String, Integer> map) {
        logger.info("\n--- 1. Basic Usage (Works like java.util.Map) ---");

        map.clear();

        // Simple put/get
        map.put("alice", 100);
        map.put("bob", 200);
        map.put("charlie", 150);
        logger.info("Added 3 users");

        logger.info("Alice's score: {}", map.get("alice"));
        logger.info("Bob's score: {}", map.get("bob"));

        // Update
        Integer oldScore = map.put("alice", 120);
        logger.info("Updated Alice: {} -> {}", oldScore, map.get("alice"));

        // Map interface methods
        map.putIfAbsent("alice", 999);  // Won't update (alice exists)
        map.putIfAbsent("dave", 175);   // Will add (dave doesn't exist)

        logger.info("Total users: {}", map.size());

        // Iteration (for more efficient iteration, see tests using streamEntries())
        logger.info("\nAll user scores:");
        map.forEach((user, score) ->
                logger.info("  {} -> {}", user, score));
    }

    /**
     * Demonstrate TTL (Time To Live) for cache-like behavior.
     */
    private static void demonstrateTTL(RedisHashMap map) {
        logger.info("\n--- 2. TTL (Auto-Expiring Keys) ---");

        map.clear();

        // Regular key (no expiration)
        map.put("permanent_user", 500);
        logger.info("Permanent key TTL: {} (−1 means no expiration)", map.getTTL("permanent_user"));

        // Key with 5-second TTL
        map.put("temp_session", 999, 5);
        Long ttl = map.getTTL("temp_session");
        logger.info("Temporary key TTL: {} seconds", ttl);
        logger.info("Value exists: {}", map.get("temp_session"));

        // Wait and check expiration
        logger.info("Waiting 6 seconds for expiration...");
        try {
            TimeUnit.SECONDS.sleep(6);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("After expiration - value: {} (should be null)", map.get("temp_session"));
        logger.info("Permanent key still exists: {}", map.get("permanent_user"));

        // Set expiration on existing key
        map.expire("permanent_user", 10);
        logger.info("Set 10s expiration on permanent_user, TTL: {}", map.getTTL("permanent_user"));

        // Remove expiration (make persistent again)
        map.persist("permanent_user");
        logger.info("After persist, TTL: {} (−1 means no expiration)", map.getTTL("permanent_user"));
    }

    /**
     * Demonstrate how Redis Cluster automatically shards data.
     */
    private static void demonstrateClusterSharding(RedisHashMap map, RedisClusterCommands<String, String> redis) {
        logger.info("\n--- 3. Redis Cluster Sharding ---");

        map.clear();

        // Add data - Lettuce automatically routes to correct nodes
        logger.info("Adding 50 keys...");
        for (int i = 0; i < 50; i++) {
            map.put("user_" + i, i * 10);
        }

        logger.info("Total keys: {}", map.size());

        // Show cluster info
        String clusterInfo = redis.clusterInfo();
        logger.info("\nCluster State:");
        for (String line : clusterInfo.split("\n")) {
            if (line.startsWith("cluster_state") ||
                    line.startsWith("cluster_slots_assigned") ||
                    line.startsWith("cluster_size")) {
                logger.info("  {}", line);
            }
        }

        // Show how specific keys map to hash slots
        logger.info("\nHash Slot Distribution Examples:");
        showKeySlot(redis, "user_scores:user_0", map);
        showKeySlot(redis, "user_scores:user_25", map);
        showKeySlot(redis, "user_scores:user_49", map);

        logger.info("\nLettuce automatically routes each operation to the correct node!");
    }

    /**
     * Show which hash slot a key belongs to.
     */
    private static void showKeySlot(RedisClusterCommands<String, String> redis, String key, RedisHashMap map) {
        Long slot = redis.clusterKeyslot(key);
        String simpleKey = key.replace("user_scores:", "");
        Integer value = map.get(simpleKey);
        logger.info("  '{}' -> Slot {} (value: {})", key, slot, value);
    }

    /**
     * Demonstrate Redis-backed List implementation.
     */
    private static void demonstrateListUsage(StatefulRedisClusterConnection<String, String> connection) {
        logger.info("\n--- 4. RedisList (List<Integer>) ---");

        RedisList recentScores = new RedisList(connection, "recent_scores");
        recentScores.clear();

        recentScores.add(10);
        recentScores.add(20);
        recentScores.add(30);
        logger.info("Recent scores: {}", recentScores);

        Integer previous = recentScores.set(1, 25);
        logger.info("Updated middle score: {} -> {}", previous, recentScores.get(1));

        Integer removed = recentScores.remove(0);
        logger.info("Removed oldest score: {}", removed);
        logger.info("Current list: {}", recentScores);
    }
}
