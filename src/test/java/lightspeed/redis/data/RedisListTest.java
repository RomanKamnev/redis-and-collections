package lightspeed.redis.data;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lightspeed.redis.config.RedisClusterConfig;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

class RedisListTest {

    private static RedisClusterClient client;
    private static StatefulRedisClusterConnection<String, String> connection;

    private RedisList redisList;
    private String key;

    @BeforeAll
    static void setupCluster() {
        client = RedisClusterConfig.createClient();
        connection = RedisClusterConfig.createConnection(client);
    }

    @AfterAll
    static void teardownCluster() {
        RedisClusterConfig.cleanup(connection, client);
    }

    @BeforeEach
    void setup() {
        key = "list_test:" + System.currentTimeMillis() + ":" + ThreadLocalRandom.current().nextInt();
        redisList = new RedisList(connection, key);
        redisList.clear();
    }

    @AfterEach
    void cleanup() {
        if (redisList != null) {
            redisList.clear();
        }
    }

    @Test
    void testAppendAndGet() {
        redisList.add(10);
        redisList.add(20);
        redisList.add(30);

        assertEquals(3, redisList.size());
        assertEquals(List.of(10, 20, 30), redisList);
    }

    @Test
    void testSetAndRemove() {
        redisList.add(5);
        redisList.add(15);
        redisList.add(25);

        Integer previous = redisList.set(1, 99);
        assertEquals(15, previous);
        assertEquals(99, redisList.get(1));

        Integer removed = redisList.remove(0);
        assertEquals(5, removed);
        assertEquals(List.of(99, 25), redisList);
    }

    @Test
    void testClear() {
        redisList.add(1);
        redisList.add(2);
        assertEquals(2, redisList.size());

        redisList.clear();
        assertEquals(0, redisList.size());
        assertTrue(redisList.isEmpty());
    }

    @Test
    void testAddUnsupportedIndex() {
        redisList.add(1);
        assertThrows(UnsupportedOperationException.class, () -> redisList.add(0, 2));
    }
}
