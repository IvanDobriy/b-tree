package ru.otus.btree.lib.api.storage;

import ru.otus.btree.lib.api.btree.IEntity;

public class Result {
    private IEntity data;
    private long position;

    public Result(IEntity data, long position) {
        this.data = data;
        this.position = position;
    }

    public IEntity getData() {
        return data;
    }

    public long getPosition() {
        return position;
    }
}
