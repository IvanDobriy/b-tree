package ru.otus.btree.api.storage;

import ru.otus.btree.api.collections.IArray;
import ru.otus.btree.api.entity.Element;
import ru.otus.btree.api.entity.IEntity;

public interface Storage {
    IArray<IEntity> findByElement(Element element);
    void insert(IEntity entity);
}
