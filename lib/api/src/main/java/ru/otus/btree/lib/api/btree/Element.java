package ru.otus.btree.lib.api.btree;

import java.util.Objects;

public class Element {
    private String name;
    private EType type;
    private Object value;

    public Element(String name, EType type, Object value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(type);

        this.name = name;
        this.value = value;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }

    public EType getType() {
        return type;
    }
}
