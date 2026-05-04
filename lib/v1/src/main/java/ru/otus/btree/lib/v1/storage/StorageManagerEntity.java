package ru.otus.btree.lib.v1.storage;

public class StorageManagerEntity {
    public static final int RECORD_SIZE = 13;

    private long id;
    private int size;
    private boolean used;

    public StorageManagerEntity(long id, int size, boolean used) {
        this.id = id;
        this.size = size;
        this.used = used;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean isUsed() {
        return used;
    }

    public void setUsed(boolean used) {
        this.used = used;
    }
}
