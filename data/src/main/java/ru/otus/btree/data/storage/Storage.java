package ru.otus.btree.data.storage;

import ru.otus.btree.domain.IStorage;
import ru.otus.btree.domain.IStorageEntityInfo;
import ru.otus.btree.domain.IStorageIndexInfo;
import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.v1.array.SingleArray;

public class Storage implements IStorage {

    @Override
    public void createIndex(String entityName, String fieldName) {
        // empty implementation
    }

    @Override
    public IArray<IStorageIndexInfo> getIndexInfo(String entityName) {
        return new SingleArray<>(0);
    }

    @Override
    public IArray<String> getEntitiesList() {
        return new SingleArray<>(0);
    }

    @Override
    public IStorageEntityInfo getEntityInfo(String name) {
        return null;
    }
}
