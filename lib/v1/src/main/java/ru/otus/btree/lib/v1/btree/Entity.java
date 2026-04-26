package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IEntity;
import ru.otus.btree.lib.api.hash.IHashTable;
import ru.otus.btree.lib.v1.array.SingleArray;
import ru.otus.btree.lib.v1.hash.OpenAddressHashTable;

import java.util.Objects;

public class Entity implements IEntity {
    private final IHashTable<String, Element> elements;
    private static final int INITIAL_SIZE = 16;
    private static final int STEP = 3;

    public Entity() {
        this.elements = new OpenAddressHashTable<>(new StringHasher(), INITIAL_SIZE, STEP);
    }

    @Override
    public Element get(String name) {
        return elements.find(name);
    }

    @Override
    public void set(Element element) {
        Objects.requireNonNull(element, "element must not be null");
        elements.remove(element.getName());
        elements.insert(element.getName(), element);
    }

    @Override
    public IArray<Element> toArray() {
        return null;
    }

    public int size() {
        return elements.size();
    }

    public boolean isEmpty() {
        return elements.size() == 0;
    }

    public IArray<Element> elements() {
        IArray<String> keys = elements.keys();
        IArray<Element> result = new SingleArray<>(0);
        for (int i = 0; i < keys.size(); i++) {
            Element element = elements.find(keys.get(i));
            if (element != null) {
                result.add(result.size(), element);
            }
        }
        return result;
    }
}
