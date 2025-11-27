package lightspeed.redis.data;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lightspeed.redis.config.RedisClusterConfig;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for RedisHashMap.
 * <p>
 * Prerequisites: Redis Cluster must be running (docker-compose up -d)
 * <p>
 * To run: mvn test
 * <p>
 * Note: These are integration tests, not unit tests. They require a real Redis Cluster.
 * For production, consider using Testcontainers to spin up Redis automatically.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RedisHashMapTest {

    private static RedisClusterClient client;
    private static StatefulRedisClusterConnection<String, String> connection;
    private RedisHashMap map;
    private String testNamespace;

    @BeforeAll
    static void setupCluster() {
        try {
            client = RedisClusterConfig.createClient();
            connection = RedisClusterConfig.createConnection(client);
        } catch (Exception e) {
            System.err.println("Failed to connect to Redis Cluster. Make sure it's running: docker-compose up -d");
            throw e;
        }
    }

    @AfterAll
    static void teardownCluster() {
        RedisClusterConfig.cleanup(connection, client);
    }

    @BeforeEach
    void setup() {
        // Use unique namespace for each test to ensure isolation
        testNamespace = "test_" + System.currentTimeMillis() + "_" + Math.random();
        map = new RedisHashMap(client, connection, testNamespace);
    }

    @AfterEach
    void cleanup() {
        // Clean up test data
        if (map != null) {
            map.clear();
        }
    }

    /**
     * Helper method to handle Redis Cluster eventual consistency.
     * Waits for the map size to stabilize at the expected value.
     * Retries up to 50 times with 100ms intervals (max 5 seconds wait).
     */
    private void waitForConsistency(int expectedSize) {
        for (int i = 0; i < 50; i++) {
            if (map.size() == expectedSize) {
                return;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("Basic put and get operations")
    void testPutAndGet() {
        // Put
        Integer oldValue = map.put("key1", 100);
        assertNull(oldValue, "First put should return null");

        // Get
        Integer value = map.get("key1");
        assertEquals(100, value, "Get should return the value we just put");

        // Update
        Integer previousValue = map.put("key1", 200);
        assertEquals(100, previousValue, "Update should return previous value");
        assertEquals(200, map.get("key1"), "Updated value should be retrievable");
    }

    @Test
    @Order(2)
    @DisplayName("Remove operation")
    void testRemove() {
        map.put("key1", 100);
        map.put("key2", 200);

        Integer removed = map.remove("key1");
        assertEquals(100, removed, "Remove should return the removed value");

        assertNull(map.get("key1"), "Removed key should not exist");
        assertEquals(200, map.get("key2"), "Other keys should not be affected");
    }

    @Test
    @Order(3)
    @DisplayName("ContainsKey operation")
    void testContainsKey() {
        map.put("key1", 100);

        assertTrue(map.containsKey("key1"), "Should contain existing key");
        assertFalse(map.containsKey("key2"), "Should not contain non-existent key");
    }

    @Test
    @Order(4)
    @DisplayName("Size operation")
    void testSize() {
        assertEquals(0, map.size(), "Empty map should have size 0");

        map.put("key1", 100);
        map.put("key2", 200);
        map.put("key3", 300);

        assertEquals(3, map.size(), "Map with 3 entries should have size 3");

        map.remove("key2");
        assertEquals(2, map.size(), "Size should decrease after removal");
    }

    @Test
    @Order(5)
    @DisplayName("Clear operation")
    void testClear() {
        map.put("key1", 100);
        map.put("key2", 200);
        map.put("key3", 300);

        assertEquals(3, map.size(), "Should have 3 entries before clear");

        map.clear();
        assertEquals(0, map.size(), "Should be empty after clear");
        assertFalse(map.containsKey("key1"), "Keys should not exist after clear");
    }

    @Test
    @Order(6)
    @DisplayName("Null key and value handling")
    void testNullHandling() {
        assertThrows(NullPointerException.class, () -> map.put(null, 100),
                "Null key should throw NPE");

        assertThrows(NullPointerException.class, () -> map.put("key", null),
                "Null value should throw NPE");

        // Get with null should return null without throwing
        assertNull(map.get(null), "Get with null key should return null");
    }

    @Test
    @Order(7)
    @DisplayName("Map interface compatibility - putIfAbsent")
    void testPutIfAbsent() {
        Integer result1 = map.putIfAbsent("key1", 100);
        assertNull(result1, "putIfAbsent on new key should return null");
        assertEquals(100, map.get("key1"), "Value should be set");

        Integer result2 = map.putIfAbsent("key1", 200);
        assertEquals(100, result2, "putIfAbsent on existing key should return existing value");
        assertEquals(100, map.get("key1"), "Value should not change");
    }

    @Test
    @Order(8)
    @DisplayName("Map interface compatibility - computeIfPresent")
    void testComputeIfPresent() {
        map.put("key1", 100);

        Integer result = map.computeIfPresent("key1", (k, v) -> v + 50);
        assertEquals(150, result, "computeIfPresent should return new value");
        assertEquals(150, map.get("key1"), "Value should be updated");

        Integer result2 = map.computeIfPresent("key2", (k, v) -> v + 50);
        assertNull(result2, "computeIfPresent on non-existent key should return null");
    }

    @Test
    @Order(9)
    @DisplayName("Map interface compatibility - replace")
    void testReplace() {
        map.put("key1", 100);

        boolean replaced = map.replace("key1", 100, 200);
        assertTrue(replaced, "Replace with correct old value should succeed");
        assertEquals(200, map.get("key1"), "Value should be updated");

        boolean notReplaced = map.replace("key1", 100, 300);
        assertFalse(notReplaced, "Replace with incorrect old value should fail");
        assertEquals(200, map.get("key1"), "Value should not change");
    }

    @Test
    @Order(10)
    @DisplayName("EntrySet iteration")
    void testEntrySet() {
        map.put("key1", 100);
        map.put("key2", 200);
        map.put("key3", 300);

        Set<Map.Entry<String, Integer>> entries = map.entrySet();
        assertEquals(3, entries.size(), "EntrySet should contain all entries");

        int sum = entries.stream()
                .mapToInt(Map.Entry::getValue)
                .sum();
        assertEquals(600, sum, "Sum of all values should be 600");
    }

    @Test
    @Order(11)
    @DisplayName("Stream entries - memory efficient iteration")
    void testStreamEntries() throws InterruptedException {
        map.put("item1", 10);
        map.put("item2", 20);
        map.put("item3", 30);
        map.put("item4", 40);
        map.put("item5", 50);

        long count = map.streamEntries().count();
        assertEquals(5, count, "Stream should contain all entries");

        int sum = map.streamEntries()
                .mapToInt(Map.Entry::getValue)
                .sum();
        assertEquals(150, sum, "Sum of streamed values should be 150");

        // Test limit
        long limited = map.streamEntries().limit(3).count();
        assertEquals(3, limited, "Limited stream should respect limit");
    }

    @Test
    @Order(12)
    @DisplayName("TTL - put with expiration")
    void testPutWithTTL() throws InterruptedException {
        // Put with 2 second TTL
        map.put("temp_key", 999, 2);

        // Key should exist immediately
        assertTrue(map.containsKey("temp_key"), "Key should exist immediately");
        assertEquals(999, map.get("temp_key"), "Value should be retrievable");

        // Check TTL
        Long ttl = map.getTTL("temp_key");
        assertNotNull(ttl, "TTL should not be null");
        assertTrue(ttl > 0 && ttl <= 2, "TTL should be between 0 and 2 seconds");

        // Wait for expiration
        TimeUnit.SECONDS.sleep(3);

        // Key should be gone
        assertFalse(map.containsKey("temp_key"), "Key should expire after TTL");
        assertNull(map.get("temp_key"), "Expired key should return null");
    }

    @Test
    @Order(13)
    @DisplayName("TTL - expire existing key")
    void testExpire() throws InterruptedException {
        // Put without TTL
        map.put("key1", 100);

        // Initially no expiration
        Long ttl1 = map.getTTL("key1");
        assertEquals(-1, ttl1, "Key without TTL should return -1");

        // Set expiration
        boolean expired = map.expire("key1", 2);
        assertTrue(expired, "Expire should succeed on existing key");

        Long ttl2 = map.getTTL("key1");
        assertTrue(ttl2 > 0 && ttl2 <= 2, "TTL should be set");

        // Wait for expiration
        TimeUnit.SECONDS.sleep(3);

        assertFalse(map.containsKey("key1"), "Key should expire");
    }

    @Test
    @Order(14)
    @DisplayName("TTL - persist (remove expiration)")
    void testPersist() {
        // Put with TTL
        map.put("key1", 100, 10);

        Long ttl1 = map.getTTL("key1");
        assertTrue(ttl1 > 0, "Key should have TTL");

        // Remove expiration
        boolean persisted = map.persist("key1");
        assertTrue(persisted, "Persist should succeed");

        Long ttl2 = map.getTTL("key1");
        assertEquals(-1, ttl2, "Persisted key should have no TTL");

        // Key should still exist
        assertEquals(100, map.get("key1"), "Persisted key should still exist");
    }

    @Test
    @Order(15)
    @DisplayName("TTL - non-existent key")
    void testTTLNonExistentKey() {
        Long ttl = map.getTTL("non_existent");
        assertEquals(-2, ttl, "Non-existent key should return -2");

        boolean expired = map.expire("non_existent", 10);
        assertFalse(expired, "Expire on non-existent key should return false");

        boolean persisted = map.persist("non_existent");
        assertFalse(persisted, "Persist on non-existent key should return false");
    }

    @Test
    @Order(16)
    @DisplayName("Performance - bulk operations (with retry for consistency)")
    void testBulkOperationsReliable() throws InterruptedException {
        int count = 1000;
        long maxWaitTimeMs = 10000; // Максимальное время ожидания (10 секунд)
        long pollIntervalMs = 200;  // Интервал между проверками (200 мс)

        // 1. Measure put performance
        long startPut = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map.put("bulk_key_" + i, i);
        }
        long putTime = System.currentTimeMillis() - startPut;

        System.out.printf("Put %d keys in %d ms (%.2f ops/sec)%n",
                count, putTime, (count * 1000.0) / putTime);

        // 2. Wait for eventual consistency and check size using retry
        long startTime = System.currentTimeMillis();
        boolean sizeMatched = false;

        while (System.currentTimeMillis() - startTime < maxWaitTimeMs) {
            int currentSize = map.size();

            if (currentSize == count) {
                sizeMatched = true;
                break;
            }

            // Логируем текущее состояние для отладки
            System.out.printf("Waiting for consistency... Current size: %d/%d%n", currentSize, count);

            // Пауза перед следующей попыткой
            TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
        }

        // 3. Final Assertion
        assertTrue(sizeMatched, String.format(
                "All keys should be inserted within %dms. Final size: %d, expected: %d",
                maxWaitTimeMs, map.size(), count));


        // 4. Measure get performance
        long startGet = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Integer value = map.get("bulk_key_" + i);
            assertEquals(i, value, "Value should match");
        }
        long getTime = System.currentTimeMillis() - startGet;

        System.out.printf("Get %d keys in %d ms (%.2f ops/sec)%n",
                count, getTime, (count * 1000.0) / getTime);

        // 5. Performance assertions
        assertTrue(putTime < 10000, "Bulk put should complete in reasonable time");
        assertTrue(getTime < 10000, "Bulk get should complete in reasonable time");
    }

}
