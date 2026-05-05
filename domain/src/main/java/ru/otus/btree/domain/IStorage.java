package ru.otus.btree.domain;

import ru.otus.btree.lib.api.array.IArray;

public interface IStorage {
    void createIndex(String entityName, String fieldName);

    IArray<IStorageIndexInfo> getIndexInfo(String entityName);

    IArray<String> getEntitiesList();

    IStorageEntityInfo getEntityInfo(String name);
}
