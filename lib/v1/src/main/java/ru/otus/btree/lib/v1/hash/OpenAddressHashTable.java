package ru.otus.btree.lib.v1.hash;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.hash.IHashTable;
import ru.otus.btree.lib.v1.array.SingleArray;


import java.util.Objects;

public class OpenAddressHashTable<K, V> implements IHashTable<K, V> {
    private class Entry {
        K key;
        V value;

        Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private final IHasher<K> hasher;
    private IArray<Entry> container;
    private IArray<Boolean> isDeleted;
    private int step;
    private int probing;
    private int size;
    private int counter;

    public OpenAddressHashTable(IHasher<K> hasher, int size, int step) {
        Objects.requireNonNull(hasher);
        this.hasher = hasher;
        container = new SingleArray<>(size);
        isDeleted = new SingleArray<>(size);
        for (int i = 0; i < isDeleted.size(); i++) {
            isDeleted.set( i, false);
        }
        this.step = step;
        probing = 0;
        this.size = 0;
        this.counter = 0;
    }

    private int getHash(K key, int i) {
        return (int) (hasher.execute(key) + i * step) % container.size();
    }


    private void rehash() {
        final double factor = (double) counter / container.size();
        if (factor > 0.7) {
            size = 0;
            probing = 0;
            IArray<Entry> oldContainer = container;
            isDeleted = new SingleArray<>(isDeleted.size() * 2);
            for (int i = 0; i < isDeleted.size(); i++) {
                isDeleted.set( i, false);
            }
            container = new SingleArray<>(oldContainer.size() * 2);
            Entry data;
            for (int i = 0; i < oldContainer.size(); i++) {
                data = oldContainer.get(i);
                if (data != null) {
                    insert(data.key, data.value);
                }
            }
        }
    }

    @Override
    public void insert(K key, V value) {
        Objects.requireNonNull(key, "key must not be null");
        for (int i = 0; i < container.size(); i++) {
            int hash = getHash(key, i);
            probing++;
            if (isDeleted.get(hash)) {
                continue;
            }
            if (container.get(hash) == null) {
                container.set(hash, new Entry(key, value));
                isDeleted.set( i, false);
                size++;
                counter++;
                rehash();
                return;
            }
        }
    }

    @Override
    public V find(K key) {
        Objects.requireNonNull(key, "key must not be null");
        Entry result;
        int index = -1;
        for (int i = 0; i < container.size(); i++) {
            int hash = getHash(key, i);
            result = container.get(hash);
            if (isDeleted.get(i)) {
                index = i;
            }
            if (result != null && result.key.equals(key)) {
                if (index >= 0 && index != i) {
                    container.set(index, result );
                    isDeleted.set(index, false);
                    container.set(i,  null);
                    isDeleted.set(i, true);
                }
                return result.value;
            }
        }
        return null;
    }

    @Override
    public void remove(K key) {
        Objects.requireNonNull(key, "key must not be null");
        Entry result;
        for (int i = 0; i < container.size(); i++) {
            int hash = getHash(key, i);
            result = container.get(hash);
            if (result != null && result.key.equals(key)) {
                container.set(hash, null);
                isDeleted.set(hash, true);
                size--;
                break;
            }
        }
    }

    @Override
    public int size() {
        return size;
    }
}
