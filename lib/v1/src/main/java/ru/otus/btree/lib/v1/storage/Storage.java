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
    public Result get(Element element) {
        Objects.requireNonNull(element, "element is null");
        int id = (int) element.getPosition();
        StorageManagerEntity managerEntity = storageManager.getEntityById(id);
        if (managerEntity == null) {
            return null;
        }
        IEntity data = rawStorage.get(managerEntity.getPosition(), managerEntity.getSize());
        if (data == null) {
            return null;
        }
        return new Result(data, managerEntity.getPosition());
    }

    @Override
    public IArray<Result> get(long from, long to) {
        SingleArray<Result> results = new SingleArray<>(0);
        for (int id = (int) from; id <= (int) to; id++) {
            StorageManagerEntity managerEntity = storageManager.getEntityById(id);
            if (managerEntity == null) {
                continue;
            }
            IEntity data = rawStorage.get(managerEntity.getPosition(), managerEntity.getSize());
            if (data == null) {
                continue;
            }
            results.add(results.size(), new Result(data, managerEntity.getPosition()));
        }
        return results;
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
