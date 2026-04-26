package ru.otus.btree.lib.api.hash;

public interface IHashTable<K, V> {
    void insert(K key, V value);
    V find(K key);
    void remove(K key);
    int size();
}
