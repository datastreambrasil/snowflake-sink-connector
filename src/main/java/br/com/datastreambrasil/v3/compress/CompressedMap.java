package br.com.datastreambrasil.v3.compress;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class CompressedMap<V> {

    private final Map<String, byte[]> store;
    private final KryoFactory kryoFactory;

    public CompressedMap(KryoFactory kryoFactory, int initialCapacity) {
        this.kryoFactory = kryoFactory;
        this.store = new LinkedHashMap<>(initialCapacity);
    }

    public void put(String key, V value) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        byte[] compressed = kryoFactory.serialize(value);
        store.put(key, compressed);
    }

    public V get(String key) {
        byte[] bytes = store.get(key);
        if (bytes == null) {
            return null;
        }
        return kryoFactory.deserialize(bytes);
    }

    public Collection<byte[]> values() {
        return store.values();
    }

    public V deserializeValue(byte[] data) {
        return kryoFactory.deserialize(data);
    }

    public void clear() {
        store.clear();
    }

    public int size() {
        return store.size();
    }

    public boolean isEmpty() {
        return store.isEmpty();
    }
}
