package timely.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class AbstractCache<T> implements IterableCache<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCache.class);
    private static final Object DUMMY = new Object();

    private volatile boolean closed = true;
    private volatile com.github.benmanes.caffeine.cache.Cache<T, Object> cache = null;

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
    public void add(T t) {
        cache.put(t, DUMMY);
    }

    @Override
    public Object get(T t) {
        return cache.getIfPresent(t);
    }

    @Override
    public boolean contains(T t) {
        return cache.asMap().containsKey(t);
    }

    @Override
    public void addAll(Collection<T> c) {
        c.forEach(t -> cache.put(t, DUMMY));
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
    public Iterator<T> iterator() {
        return cache.asMap().keySet().iterator();
    }
}
