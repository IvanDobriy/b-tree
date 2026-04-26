package ru.otus.btree.lib.api.btree;

import ru.otus.btree.lib.api.array.IArray;

public interface IEntity {
    Element get(String name);

    void set(Element element);

    IArray<Element> toArray();
}
