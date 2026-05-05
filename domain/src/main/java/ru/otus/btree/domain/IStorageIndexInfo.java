package ru.otus.btree.domain;

public interface IStorageIndexInfo {
    String getFieldName();

    long getSize();

    long getFileSize();
}
