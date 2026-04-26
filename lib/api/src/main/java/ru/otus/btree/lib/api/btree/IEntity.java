package ru.otus.btree.lib.api.btree;

public interface IEntity {
    Element get(String name);

    void set(Element element);
}
