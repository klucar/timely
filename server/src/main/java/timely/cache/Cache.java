package timely.cache;

import timely.Configuration;

import java.util.Collection;
import java.util.Map;

/**
 *
 */
public interface Cache<K, V> {

    void initialize(Configuration config);

    void add(K k, V v);

    V get(K k);

    boolean contains(K key);

    void addAll(Map<K, V> m);

    void close();

    boolean isClosed();

}
