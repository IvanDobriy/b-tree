package ru.otus.btree.domain;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IEntity;
import ru.otus.btree.lib.api.storage.Result;

public interface IStorage {
    void createIndex(String entityName, String fieldName);

    IArray<Result> findByIndex(String entityName, Element element);

    IArray<IStorageIndexInfo> getIndexInfo(String entityName);

    void createEntityStorage(String name);

    void setEntity(String name, IEntity entity);

    void setEntities(String name, IArray<IEntity> entities);

    IEntity getEntity(String name, int position);

    IArray<String> getEntitiesList();

    IStorageEntityInfo getEntityInfo(String name);
}
