package ru.otus.btree.api.entity;

public interface IEntity {
    Element get(String name);

    void set(Element element);

    IArray<Element> toArray();
}
