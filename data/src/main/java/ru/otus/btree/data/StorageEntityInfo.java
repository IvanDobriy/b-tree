package ru.otus.btree.data;

import ru.otus.btree.domain.IStorageEntityInfo;

public class StorageEntityInfo implements IStorageEntityInfo {
    private final String name;
    private final long size;
    private final long fileSize;

    public StorageEntityInfo(String name, long size, long fileSize) {
        this.name = name;
        this.size = size;
        this.fileSize = fileSize;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public long getFileSize() {
        return fileSize;
    }
}
