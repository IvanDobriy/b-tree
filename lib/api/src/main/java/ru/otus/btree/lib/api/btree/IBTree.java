package ru.otus.btree.lib.api.btree;


import ru.otus.btree.lib.api.array.IArray;

public interface IBTree {
    void insert(String keyName, IEntity entity, long entityPosition);

    IArray<Element> search(Element element);

    void delete(Element element);
}
