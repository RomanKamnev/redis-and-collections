package lightspeed.redis.data;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractList;
import java.util.List;
import java.util.Objects;

/**
 * Minimal List<Integer> implementation backed by a Redis list.
 * <p>
 * This class focuses on the most common operations (append, get by index,
 * set by index, remove by index, clear). Random insertion/removal requires
 * re-writing the list and is therefore implemented in a simple but expensive
 * way using LRANGE + DEL + RPUSH.
 */
public class RedisList extends AbstractList<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(RedisList.class);

    private final RedisClusterCommands<String, String> redis;
    private final String key;

    /**
     * Create Redis-backed list using cluster connection.
     *
     * @param connection cluster connection
     * @param key        redis key that stores this list (already namespaced)
     */
    public RedisList(StatefulRedisClusterConnection<String, String> connection, String key) {
        this.redis = connection.sync();
        this.key = key;
    }

    /**
     * Legacy constructor for compatibility with pre-existing RedisClusterCommands.
     * Note: works only with single-node operations.
     */
    public RedisList(RedisClusterCommands<String, String> redis, String key) {
        this.redis = redis;
        this.key = key;
    }

    @Override
    public Integer get(int index) {
        validateIndex(index);
        String value = redis.lindex(key, index);
        if (value == null) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
        return Integer.parseInt(value);
    }

    @Override
    public int size() {
        Long len = redis.llen(key);
        return len != null ? Math.toIntExact(Math.min(len, (long) Integer.MAX_VALUE)) : 0;
    }

    @Override
    public boolean add(Integer integer) {
        Objects.requireNonNull(integer, "RedisList does not support null values");
        redis.rpush(key, integer.toString());
        return true;
    }

    @Override
    public void add(int index, Integer element) {
        Objects.requireNonNull(element, "RedisList does not support null values");

        if (index != size()) {
            throw new UnsupportedOperationException("RedisList only supports append operations");
        }

        add(element);
    }

    @Override
    public Integer set(int index, Integer element) {
        Objects.requireNonNull(element, "RedisList does not support null values");

        validateIndex(index);
        String existing = redis.lindex(key, index);
        if (existing == null) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }

        redis.lset(key, index, element.toString());
        return Integer.parseInt(existing);
    }

    @Override
    public Integer remove(int index) {
        List<String> values = redis.lrange(key, 0, -1);
        if (index < 0 || index >= values.size()) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }

        String removed = values.remove(index);

        // Rewrite the list: delete the key and reinsert the remaining values
        redis.del(key);
        if (!values.isEmpty()) {
            redis.rpush(key, values.toArray(new String[0]));
        }

        return Integer.parseInt(removed);
    }

    @Override
    public void clear() {
        redis.del(key);
    }

    private void validateIndex(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index: " + index);
        }
    }
}
