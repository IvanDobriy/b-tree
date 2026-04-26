package ru.otus.btree.lib.v1.hash;

public interface IHasher<K> {
    long execute(K key);
}
