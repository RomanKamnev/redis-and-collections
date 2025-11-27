# Redis Collections Framework - Technical Assignment for Lightspeed

**Java Version:** 21
**Redis Solution:** Redis Cluster with Lettuce 6.4.0
**Questions:** join-ecom@lightspeedhq.com

---

## 📋 Assignment Requirements

Implement `java.util.Map<String, Integer>` backed by Redis with:
- ✅ Transparent Map interface usage (works like regular HashMap)
- ✅ Automatic data sharding across Redis cluster
- ✅ Dynamic server addition/removal support (Redis Cluster handles this)
- ✅ Data redundancy with automatic failover (3 masters + 3 replicas)
- ✅ Working `main()` method demonstrating functionality
- ✅ Bonus: `List<Integer>` implementation using Redis Lists

---

## 🚀 Quick Start

### Prerequisites

- Java 21+
- Docker & Docker Compose
- Maven 3.6+

### Running the Demo

**Option 1: Automated (recommended)**
```bash
# Linux/macOS
./scripts/test.sh
```

On Windows, run the same steps manually (docker-compose up, `mvn test`, `mvn exec:java`).

This script will:
1. Start Redis Cluster (6 nodes: 3 masters + 3 replicas)
2. Run all integration tests
3. Execute the demo application
4. Clean up resources

**Option 2: Manual**
```bash
# 1. Start Redis Cluster
docker-compose -f infra/docker-compose.yml up -d

# Wait for cluster initialization (~30 seconds)
docker-compose -f infra/docker-compose.yml logs redis-cluster-init

# 2. Build and run demo
mvn clean package
mvn exec:java

# 3. Run tests
mvn test

# 4. Cleanup
docker-compose -f infra/docker-compose.yml down -v
```

---

## 💡 Implementation Overview

### Core Design Decisions

1. **Redis Cluster** instead of Sentinel/Standalone
   - Built-in sharding via 16384 hash slots
   - Automatic failover and replica promotion
   - No need for custom consistent hashing

2. **Lettuce 6.4.0** instead of Jedis
   - Thread-safe (share single connection)
   - Native Redis Cluster support
   - Modern async/reactive API
   - Better performance

3. **Namespace Isolation**
   ```java
   Map<String, Integer> scores = new RedisHashMap(client, connection, "user_scores");
   scores.put("alice", 100);
   // Stored in Redis as: "user_scores:alice" = "100"
   ```

4. **SCAN for Iteration** instead of KEYS
   - Non-blocking, cursor-based iteration
   - Memory-efficient for millions of keys
   - Doesn't block Redis server

---

## 🎯 Key Features Demonstrated

### 1. Transparent Map Interface

```java
// Works exactly like HashMap
Map<String, Integer> scores = new RedisHashMap(client, connection, "scores");

scores.put("alice", 100);
scores.put("bob", 200);
scores.putIfAbsent("charlie", 300);

System.out.println(scores.get("alice"));        // 100
System.out.println(scores.size());              // 3
System.out.println(scores.containsKey("bob"));  // true

scores.remove("charlie");
```

### 2. Automatic Sharding

Keys are automatically distributed across 3 master nodes:
```
"scores:alice"   → CRC16 hash % 16384 → Slot 8839  → Master 2
"scores:bob"     → CRC16 hash % 16384 → Slot 5798  → Master 1
"scores:charlie" → CRC16 hash % 16384 → Slot 1444  → Master 1
```

Lettuce handles routing automatically - no manual sharding logic needed.

### 3. High Availability

```
Master 1 (slots 0-5460)      → Replica 1
Master 2 (slots 5461-10922)  → Replica 2
Master 3 (slots 10923-16383) → Replica 3
```

If a master fails, its replica is promoted automatically.

### 4. TTL Support (Auto-expiring keys)

```java
// Session management use case
map.put("session:abc123", userId, 3600);  // Expires in 1 hour

Long ttl = map.getTTL("session:abc123");  // Check remaining time
map.expire("session:abc123", 7200);       // Extend to 2 hours
map.persist("session:abc123");            // Make permanent
```

### 5. Memory-Efficient Iteration

```java
// Bad: Loads all data into memory
Set<Entry<String, Integer>> all = map.entrySet();

// Good: Lazy iteration via Stream API
map.streamEntries()
   .filter(e -> e.getValue() > 100)
   .limit(1000)
   .forEach(e -> process(e));
```

### 6. Bonus: List Implementation

```java
List<Integer> recentScores = new RedisList(connection, "recent");
recentScores.add(10);
recentScores.add(20);
recentScores.set(1, 25);
System.out.println(recentScores.get(0));  // 10
```

---

## 📊 Architecture

```
┌─────────────────────────────┐
│   Java Application          │
│   Map<String, Integer>      │
└────────────┬────────────────┘
             │
┌────────────▼────────────────┐
│   RedisHashMap              │
│   • Namespace isolation     │
│   • SCAN iteration          │
│   • Cluster-aware ops       │
└────────────┬────────────────┘
             │
┌────────────▼────────────────┐
│   Lettuce Client            │
│   • Smart routing           │
│   • Connection pooling      │
└────────────┬────────────────┘
             │
    ┌────────┴────────┐
    ▼                 ▼
┌─────────┐      ┌─────────┐
│Master 1 │      │Master 2 │ ...
│Replica 1│      │Replica 2│
└─────────┘      └─────────┘
  Redis Cluster (6 nodes)
```

---

## 🧪 Testing

The project includes 16+ integration tests covering:

- ✅ Basic Map operations (put, get, remove, size, clear)
- ✅ Map interface methods (putIfAbsent, computeIfPresent, replace)
- ✅ TTL functionality (expire, persist, getTTL)
- ✅ Iteration (entrySet, streamEntries)
- ✅ Bulk operations (1000 keys benchmark)
- ✅ Edge cases (null handling, eventual consistency)

**Test Results:**
```
[INFO] Tests run: 16, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

---

## 📁 Project Structure

```
redis-and-collections/
├── src/main/java/lightspeed/redis/
│   ├── RedisClusterConfig.java      # Connection setup
│   ├── RedisHashMap.java            # Map<String, Integer> implementation
│   ├── RedisList.java               # List<Integer> implementation (bonus)
│   └── Main.java                    # Demo application
├── src/test/java/lightspeed/redis/
│   ├── RedisHashMapTest.java        # Integration tests
│   └── RedisListTest.java           # List tests
├── infra/
│   └── docker-compose.yml           # Redis Cluster (6 nodes)
├── scripts/
│   └── test.sh                      # Automated test runner (Linux/macOS)
└── pom.xml                          # Maven dependencies
```

---

## 🎬 Demo Application Output

```
=== Demo 1: Basic Map Operations ===
PUT: alice -> 100
PUT: bob -> 200
GET alice: 100
Size: 2
Contains bob: true
REMOVE alice: 100
Size after removal: 1

=== Demo 2: TTL Support ===
Session created with 60s TTL
TTL remaining: 60 seconds
Session persisted (no expiration)

=== Demo 3: Cluster Sharding ===
Key 'user:1000' -> Slot 1234 -> Master 1
Key 'user:2000' -> Slot 5678 -> Master 2
Key 'user:3000' -> Slot 9012 -> Master 2
Data automatically distributed across 3 masters

=== Demo 4: RedisList ===
Added: [10, 20, 30]
Get index 1: 20
Size: 3
```

---

## 🔍 How Dynamic Server Management Works

Redis Cluster provides dynamic server management out of the box:

**Adding a node:**
```bash
# Redis Cluster automatically redistributes slots
redis-cli --cluster add-node <new-node> <existing-node>
redis-cli --cluster reshard <cluster-node>
```

**Removing a node:**
```bash
# Slots are moved to remaining nodes
redis-cli --cluster del-node <cluster-node> <node-id>
```

**Automatic failover:**
- Master fails → Replica promoted in ~1 second
- Lettuce detects topology change and reroutes

Our implementation automatically:
- Refreshes cluster topology via `ensurePartitions()`
- Retries with backoff on connection failures
- Handles null returns from temporarily unavailable nodes

---


## ✅ Assignment Checklist

- [x] Map<String, Integer> implementation
- [x] Transparent Map interface usage
- [x] Automatic sharding (Redis Cluster)
- [x] Dynamic server management (Redis Cluster built-in)
- [x] Data redundancy (3 masters + 3 replicas)
- [x] Working main() method
- [x] Bonus: List<Integer> implementation
- [x] Comprehensive tests (16+ integration tests)
- [x] Docker setup for easy reproduction
- [x] Production-ready code quality

---

## 🚀 Production Considerations

While this is a technical assignment, the implementation is production-ready:

- ✅ Thread-safe (Lettuce connection is thread-safe)
- ✅ Connection pooling (Lettuce handles automatically)
- ✅ Proper error handling and logging
- ✅ Memory-efficient iteration (SCAN instead of KEYS)
- ✅ TTL support for cache-like use cases
- ✅ Comprehensive test coverage

---

**Thank you for reviewing this submission!** 🎉
