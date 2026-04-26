package ru.otus.btree.lib.api.btree;


public interface IBTree<V> {
    void insert(String keyName, IEntity entity);

    IEntity search(Element element);

    void delete(Element element);
}
