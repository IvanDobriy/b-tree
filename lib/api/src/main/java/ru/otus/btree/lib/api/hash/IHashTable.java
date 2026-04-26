package ru.otus.btree.lib.api.hash;

import ru.otus.btree.lib.api.array.IArray;

public interface IHashTable<K, V> {
    void insert(K key, V value);
    V find(K key);
    void remove(K key);
    int size();
    IArray<K> keys();
}
