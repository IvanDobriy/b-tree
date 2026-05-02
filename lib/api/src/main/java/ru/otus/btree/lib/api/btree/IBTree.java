package ru.otus.btree.lib.api.btree;


public interface IBTree<V> {
    void insert(String keyName, IEntity entity, long position);

    Element search(Element element);

    void delete(Element element);
}
