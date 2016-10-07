package timely.cache;

import timely.Configuration;

import java.util.Collection;

/**
 *
 */
public interface Cache<T> {

    void initialize(Configuration config);

    void add(T t);

    Object get(T t);

    boolean contains(T t);

    void addAll(Collection<T> c);

    void close();

    boolean isClosed();

}
