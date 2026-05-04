package ru.otus.btree.lib.api.storage;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IEntity;


public interface IStorage {
    Result get(Element element);

    IArray<Result> get(long from, long to);

    void insert(IArray<IEntity> entity);

    void remove(Element element);

    long size();

    long fileSize();
}
