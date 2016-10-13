package timely.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class AbstractCache<K, V> implements IterableCache<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCache.class);
    private static final Object DUMMY = new Object();

    private volatile boolean closed = true;
    private volatile Cache<K, V> cache = null;

    public void initialize(long expirationMinutes, int initialCapacity, long maxCapacity) {
        LOG.info("Initializing Cache");
        if (cache == null) {
            cache = Caffeine.newBuilder().expireAfterAccess(expirationMinutes, TimeUnit.MINUTES)
                    .initialCapacity(initialCapacity).maximumSize(maxCapacity).build();
            closed = false;
            LOG.info("Cache initialized. initialCapacity: {} maxCapacity: {} expirationMinutes: {} ", initialCapacity,
                    maxCapacity, expirationMinutes);
        } else {
            LOG.info("Cache already initialized");
        }
    }

    public void initialize(long expirationMinutes) {
        LOG.info("Initializing Cache");
        if (cache == null) {
            cache = Caffeine.newBuilder().expireAfterAccess(expirationMinutes, TimeUnit.MINUTES).build();
            closed = false;
            LOG.info("Cache initialized. expirationMinutes: {} ", expirationMinutes);
        } else {
            LOG.info("Cache already initialized");
        }
    }

    @Override
    public abstract void initialize(Configuration config);

    @Override
    public void add(K k, V v) {
        cache.put(k, v);
    }

    @Override
    public V get(K k) {
        return cache.getIfPresent(k);
    }

    @Override
    public boolean contains(K key) {
        return cache.asMap().containsKey(key);
    }

    @Override
    public void addAll(Map<K, V> map) {
        cache.putAll(map);
    }

    @Override
    public void close() {
        cache.invalidateAll();
        cache.cleanUp();
        cache = null;
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public Iterator<V> iterator() {
        return cache.asMap().values().iterator();
    }
}
