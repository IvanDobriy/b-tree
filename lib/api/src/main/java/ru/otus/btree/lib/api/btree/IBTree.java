package ru.otus.btree.lib.api.btree;


import ru.otus.btree.lib.api.array.IArray;

public interface IBTree<V> {
    void insert(String keyName, IEntity entity);

    IArray<Element> search(Element element);

    void delete(Element element);
}
