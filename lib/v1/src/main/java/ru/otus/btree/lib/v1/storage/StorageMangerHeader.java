package ru.otus.btree.lib.v1.storage;

public class StorageMangerHeader {
    private long size;

    public StorageMangerHeader(long size) {
        this.size = size;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}
