package ru.otus.btree.lib.v1.storage;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IEntity;
import ru.otus.btree.lib.api.storage.IStorage;
import ru.otus.btree.lib.api.storage.Result;
import ru.otus.btree.lib.v1.array.SingleArray;
import ru.otus.btree.lib.v1.btree.FileBTreeUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class Storage implements IStorage {
    private final StorageManager storageManager;
    private final RawStorage rawStorage;
    private long size;

    public Storage(FileChannel dataChannel, FileChannel metaChannel) {
        this.rawStorage = new RawStorage(Objects.requireNonNull(dataChannel, "data channel is null"));
        this.storageManager = new StorageManager(Objects.requireNonNull(metaChannel, "meta channel is null"));
        this.size = 0;
    }

    @Override
    public IArray<Result> get(Element element) {
        // TODO: implement indexed search
        return new SingleArray<>(0);
    }

    @Override
    public IArray<Result> get(long from, long to) {
        // TODO: implement range scan
        return new SingleArray<>(0);
    }

    @Override
    public void insert(IArray<IEntity> entity) {
        Objects.requireNonNull(entity, "entity array is null");
    }

    @Override
    public void remove(Element element) {
        throw new RuntimeException("not yet implemented");
    }

    @Override
    public long size() {
        return size;
    }

}
