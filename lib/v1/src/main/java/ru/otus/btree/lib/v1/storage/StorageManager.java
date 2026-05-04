package ru.otus.btree.lib.v1.storage;

import java.nio.channels.FileChannel;
import java.util.Objects;

public class StorageManager {
    private static final int PAGE_SIZE = 4096;
    private final StorageMangerList storageMangerList;

    public StorageManager(FileChannel fileChannel) {
        Objects.requireNonNull(fileChannel, "file channel is null");
        this.storageMangerList = new StorageMangerList(fileChannel);
    }

}
