package ru.otus.btree.data.storage;

import ru.otus.btree.domain.IStorageIndexInfo;

public class StorageIndexInfo implements IStorageIndexInfo {
    private final String fieldName;
    private final long size;
    private final long fileSize;

    public StorageIndexInfo(String fieldName, long size, long fileSize) {
        this.fieldName = fieldName;
        this.size = size;
        this.fileSize = fileSize;
    }

    @Override
    public String getFieldName() {
        return fieldName;
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
