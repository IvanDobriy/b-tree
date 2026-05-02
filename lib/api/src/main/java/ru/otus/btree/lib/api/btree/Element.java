package ru.otus.btree.lib.api.btree;

import java.util.Objects;

public class Element {
    private String name;
    private EType type;
    private Object value;
    private long position;

    public Element(String name, EType type, Object value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(type);

        this.name = name;
        this.value = value;
        this.type = type;
        this.position = -1;
    }

    public Element(String name, EType type, Object value, long position) {
        this(name, type, value);
        this.position = position;
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

    public long getPosition() {
        return position;
    }
}
