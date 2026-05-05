package ru.otus.btree.domain;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IEntity;

public interface IStorage {
    void createIndex(String entityName, String fieldName);

    IArray<IStorageIndexInfo> getIndexInfo(String entityName);

    void createEntityStorage(String name);

    void setEntity(String name, IEntity entity);

    IEntity getEntity(String name, int position);

    IArray<String> getEntitiesList();

    IStorageEntityInfo getEntityInfo(String name);
}
