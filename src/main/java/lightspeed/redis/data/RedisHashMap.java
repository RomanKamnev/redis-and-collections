package lightspeed.redis.data;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Implementation of java.util.Map&lt;String, Integer&gt; backed by Redis Cluster.
 * <p>
 * This implementation correctly handles Redis Cluster by scanning all master nodes
 * for operations like size(), clear(), entrySet(), etc.
 * <p>
 * Important notes:
 * - Keys are namespaced to avoid collisions
 * - size() and entrySet() are expensive O(N) operations that scan all master nodes
 * - Use streamEntries() for memory-efficient iteration
 */
public class RedisHashMap extends AbstractMap<String, Integer> {

    private static final Logger logger = LoggerFactory.getLogger(RedisHashMap.class);

    private final RedisClusterClient clusterClient;
    private final StatefulRedisClusterConnection<String, String> connection;
    private final RedisClusterCommands<String, String> redis;
    private final String namespace;

    /**
     * Create a new RedisHashMap.
     *
     * @param connection Lettuce cluster connection (thread-safe)
     * @param namespace  namespace prefix for keys (e.g., "user_scores")
     */
    public RedisHashMap(RedisClusterClient clusterClient,
                        StatefulRedisClusterConnection<String, String> connection,
                        String namespace) {
        this.clusterClient = clusterClient;
        this.connection = connection;
        this.redis = connection.sync();
        this.namespace = namespace;
        logger.info("Created RedisHashMap with namespace: {}", namespace);

        try {
            this.redis.clusterNodes();
        } catch (Exception e) {
            logger.warn("Failed to refresh cluster topology during initialization", e);
        }
    }

    public RedisHashMap(StatefulRedisClusterConnection<String, String> connection, String namespace) {
        this(null, connection, namespace);
        logger.warn("RedisHashMap instantiated without RedisClusterClient; cluster-wide scans fall back to per-connection behavior");
    }

    /**
     * Legacy constructor for backward compatibility.
     * Note: This constructor is less efficient as it doesn't have access to cluster topology.
     */
    public RedisHashMap(RedisClusterCommands<String, String> redis, String namespace) {
        this.clusterClient = null;
        this.connection = null;
        this.redis = redis;
        this.namespace = namespace;
        logger.warn("Created RedisHashMap without cluster connection - some operations may not work correctly across all nodes");
    }

    /**
     * Build Redis key with namespace.
     */
    private String buildKey(String key) {
        return namespace + ":" + key;
    }

    /**
     * Extract original key from namespaced Redis key.
     */
    private String extractKey(String redisKey) {
        return redisKey.substring(namespace.length() + 1);
    }

    /**
     * Get all master nodes from the cluster.
     */
    private Collection<RedisClusterNode> getMasterNodes() {
        Partitions partitions = ensurePartitions();

        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalStateException("CRITICAL: No cluster partitions available after refresh attempts");
        }

        return partitions.stream()
                .filter(node -> node.is(RedisClusterNode.NodeFlag.UPSTREAM)
                        || node.is(RedisClusterNode.NodeFlag.MASTER))
                .collect(Collectors.toList());
    }

    private Partitions ensurePartitions() {
        Partitions partitions = null;
//         Retry a few times because the connection might not have discovered the cluster topology yet.
//         After each attempt we force a topology refresh and wait briefly.
        for (int attempt = 0; attempt < 10; attempt++) {
            partitions = connection != null ? connection.getPartitions() : null;

            if (partitions != null && !partitions.isEmpty()) {
                break;
            }

            try {
                if (clusterClient != null) {
                    clusterClient.reloadPartitions();
                }
            } catch (Exception e) {
                logger.warn("Unable to reload partitions via clusterClient", e);
            }

            try {
                if (connection != null) {
                    redis.clusterNodes();
                }
            } catch (Exception e) {
                logger.warn("Unable to refresh partitions via connection", e);
            }

            if (attempt < 9) {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        return partitions;
    }

    @Override
    public Integer get(Object key) {
        if (!(key instanceof String)) {
            return null;
        }

        String redisKey = buildKey((String) key);
        String value = redis.get(redisKey);

        return value != null ? Integer.parseInt(value) : null;
    }

    @Override
    public Integer put(String key, Integer value) {
        if (key == null || value == null) {
            throw new NullPointerException("RedisHashMap does not support null keys or values");
        }

        String redisKey = buildKey(key);

        // Get old value before setting new one
        String oldValue = redis.getset(redisKey, value.toString());

        return oldValue != null ? Integer.parseInt(oldValue) : null;
    }

    /**
     * Put a key-value pair with TTL (Time To Live).
     * The key will automatically expire after the specified number of seconds.
     *
     * @param key        the key
     * @param value      the value
     * @param ttlSeconds time to live in seconds
     * @return previous value or null
     */
    public Integer put(String key, Integer value, long ttlSeconds) {
        if (key == null || value == null) {
            throw new NullPointerException("RedisHashMap does not support null keys or values");
        }

        String redisKey = buildKey(key);

        // Get old value
        String oldValue = redis.get(redisKey);

        // Set with TTL using SETEX
        redis.setex(redisKey, ttlSeconds, value.toString());

        return oldValue != null ? Integer.parseInt(oldValue) : null;
    }

    /**
     * Get the remaining time to live for a key in seconds.
     *
     * @param key the key
     * @return TTL in seconds, -1 if no expiration, -2 if key doesn't exist
     */
    public Long getTTL(String key) {
        String redisKey = buildKey(key);
        return redis.ttl(redisKey);
    }

    /**
     * Set expiration time for an existing key.
     *
     * @param key        the key
     * @param ttlSeconds time to live in seconds
     * @return true if expiration was set, false if key doesn't exist
     */
    public boolean expire(String key, long ttlSeconds) {
        String redisKey = buildKey(key);
        return redis.expire(redisKey, ttlSeconds);
    }

    /**
     * Remove expiration from a key, making it persistent.
     *
     * @param key the key
     * @return true if expiration was removed, false if key doesn't exist or has no expiration
     */
    public boolean persist(String key) {
        String redisKey = buildKey(key);
        return redis.persist(redisKey);
    }

    @Override
    public Integer remove(Object key) {
        if (!(key instanceof String)) {
            return null;
        }

        String redisKey = buildKey((String) key);

        // GETDEL is atomic get-and-delete (Redis 6.2+)
        String oldValue = redis.getdel(redisKey);

        return oldValue != null ? Integer.parseInt(oldValue) : null;
    }

    @Override
    public boolean containsKey(Object key) {
        if (!(key instanceof String)) {
            return false;
        }

        String redisKey = buildKey((String) key);
        return redis.exists(redisKey) > 0;
    }

    @Override
    public void clear() {
        logger.warn("clear() is scanning all master nodes - this is expensive!");

        Collection<RedisClusterNode> masterNodes = getMasterNodes();

        if (masterNodes.isEmpty()) {
            clearSingleNode(redis);
            return;
        }

        for (RedisClusterNode node : masterNodes) {
            executeOnNode(node, commands -> {
                clearSingleNode(commands);
                return null;
            });
        }

        logger.info("Cleared all keys in namespace: {} across {} master nodes", namespace, masterNodes.size());
    }

    /**
     * Clear keys on a single node.
     * Delete keys one by one to avoid CROSSSLOT errors in cluster mode.
     */
    private void clearSingleNode(RedisClusterCommands<String, String> commands) {
        ScanArgs scanArgs = ScanArgs.Builder.matches(namespace + ":*").limit(100);
        KeyScanCursor<String> cursor = commands.scan(scanArgs);

        while (true) {
            // Process current batch
            List<String> keys = cursor.getKeys();
            for (String key : keys) {
                commands.del(key);
            }

            // Check if done
            if (cursor.isFinished()) {
                break;
            }

            // Fetch next batch
            cursor = commands.scan(cursor, scanArgs);
        }
    }

    /**
     * WARNING: This is an O(N) operation that scans all master nodes across the cluster.
     * Not recommended for production use with large datasets.
     *
     * @return exact count of keys (expensive)
     */
    @Override
    public int size() {
        logger.warn("size() is O(N) - scanning all master nodes across cluster");

        Collection<RedisClusterNode> masterNodes = getMasterNodes();

        if (masterNodes.isEmpty()) {
            // Fallback to single-node scan for backward compatibility
            return sizeSingleNode(redis);
        }

        int totalCount = 0;

        for (RedisClusterNode node : masterNodes) {
            totalCount += executeOnNode(node, this::sizeSingleNode);
        }

        return totalCount;
    }

    /**
     * Count keys on a single node.
     */
    private int sizeSingleNode(RedisClusterCommands<String, String> commands) {
        int count = 0;
        ScanArgs scanArgs = ScanArgs.Builder.matches(namespace + ":*").limit(100);
        KeyScanCursor<String> cursor = commands.scan(scanArgs);

        while (true) {
            // Count current batch
            count += cursor.getKeys().size();

            // Check if done
            if (cursor.isFinished()) {
                break;
            }

            // Fetch next batch
            cursor = commands.scan(cursor, scanArgs);
        }

        return count;
    }

    /**
     * WARNING: This loads all data into memory from all master nodes!
     * Not recommended for production use with large datasets.
     * <p>
     * Use streamEntries() for memory-efficient iteration.
     *
     * @return entry set (expensive)
     */
    @Override
    public Set<Entry<String, Integer>> entrySet() {
        logger.warn("entrySet() loads all data into memory - use streamEntries() instead");

        Collection<RedisClusterNode> masterNodes = getMasterNodes();

        if (masterNodes.isEmpty()) {
            // Fallback to single-node scan for backward compatibility
            return entrySetSingleNode(redis);
        }

        Set<Entry<String, Integer>> allEntries = new HashSet<>();

        // Collect entries from each master node
        for (RedisClusterNode node : masterNodes) {
            allEntries.addAll(executeOnNode(node, this::entrySetSingleNode));
        }

        return allEntries;
    }

    /**
     * Get entry set from a single node.
     */
    private Set<Entry<String, Integer>> entrySetSingleNode(RedisClusterCommands<String, String> commands) {
        Set<Entry<String, Integer>> entries = new HashSet<>();
        ScanArgs scanArgs = ScanArgs.Builder.matches(namespace + ":*").limit(100);
        KeyScanCursor<String> cursor = commands.scan(scanArgs);

        while (true) {
            // Process current batch
            List<String> keys = cursor.getKeys();

            for (String redisKey : keys) {
                String value = commands.get(redisKey);
                if (value != null) {
                    String originalKey = extractKey(redisKey);
                    entries.add(new SimpleEntry<>(originalKey, Integer.parseInt(value)));
                }
            }

            // Check if done
            if (cursor.isFinished()) {
                break;
            }

            // Fetch next batch
            cursor = commands.scan(cursor, scanArgs);
        }

        return entries;
    }

    /**
     * Memory-efficient streaming of entries using SCAN across all master nodes.
     * Use this instead of entrySet() for large datasets.
     *
     * @return stream of entries
     */
    public Stream<Entry<String, Integer>> streamEntries() {
        Collection<RedisClusterNode> masterNodes = getMasterNodes();

        if (masterNodes.isEmpty()) {
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(new ScanIterator(redis), Spliterator.ORDERED),
                    false
            );
        }

        return masterNodes.stream()
                .map(node -> {
                    Stream<Entry<String, Integer>> nodeStream = executeOnNode(node, commands ->
                            StreamSupport.stream(
                                    Spliterators.spliteratorUnknownSize(new ScanIterator(commands), Spliterator.ORDERED),
                                    false
                            )
                    );
                    return nodeStream != null ? nodeStream : Stream.<Entry<String, Integer>>empty();
                })
                .flatMap(Function.identity());
    }

    private <T> T executeOnNode(RedisClusterNode node, Function<RedisClusterCommands<String, String>, T> action) {
        try {
            RedisClusterCommands<String, String> nodeCommands = connection.getConnection(node.getNodeId()).sync();
            return action.apply(nodeCommands);
        } catch (Exception e) {
            logger.debug("Node {} temporarily unavailable during scan: {}", node.getNodeId(), e.toString());
            return null; // caller must handle null → we do it above
        }
    }

    private class ScanIterator implements Iterator<Entry<String, Integer>> {
        private final RedisClusterCommands<String, String> commands;
        private KeyScanCursor<String> cursor;
        private final ScanArgs scanArgs;
        private Iterator<String> currentBatch;

        ScanIterator(RedisClusterCommands<String, String> commands) {
            this.commands = commands;
            this.scanArgs = ScanArgs.Builder.matches(namespace + ":*").limit(100);
            this.cursor = commands.scan(scanArgs);
            this.currentBatch = cursor.getKeys().iterator();
        }

        @Override
        public boolean hasNext() {
            // Keep scanning until we find a non-empty batch or finish
            while (!currentBatch.hasNext()) {
                if (cursor.isFinished()) {
                    return false;
                }

                cursor = commands.scan(cursor, scanArgs);
                currentBatch = cursor.getKeys().iterator();
            }

            return true;
        }

        @Override
        public Entry<String, Integer> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            String redisKey = currentBatch.next();
            String value = commands.get(redisKey);

            if (value == null) {
                return next();
            }

            return new SimpleEntry<>(extractKey(redisKey), Integer.parseInt(value));
        }
    }

}
