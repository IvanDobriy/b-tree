package ru.otus.btree.api.entity;

public enum EType {

    STRING(0),
    INTEGER(1);

    private int type;

    private EType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

}
