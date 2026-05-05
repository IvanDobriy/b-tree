package ru.otus.btree.data.storage;

import ru.otus.btree.domain.IStorage;
import ru.otus.btree.domain.IStorageEntityInfo;
import ru.otus.btree.domain.IStorageIndexInfo;
import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.IBTree;
import ru.otus.btree.lib.api.btree.IEntity;
import ru.otus.btree.lib.api.storage.Result;
import ru.otus.btree.lib.v1.array.SingleArray;
import ru.otus.btree.lib.v1.btree.FileBTree;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class Storage implements IStorage {
    private final static int B_TREE_DEGREE = 1024;
    private final Path path;
    private Path storageMetaEntitiyListPath;
    private Path storageDataPath;

    private interface Callback<V, R> {
        R call(V value);
    }

    public Storage(Path path) {
        this.path = Objects.requireNonNull(path, "path is null");
        try {
            Path storagePath = path.resolve("./storage");
            if (!Files.exists(storagePath)) {
                Files.createDirectories(path);
            }
            storageMetaEntitiyListPath = storagePath.resolve("./meta/storageList");
            if (!Files.exists(storageMetaEntitiyListPath)) {
                Files.createFile(storageMetaEntitiyListPath);
            }
            withStorageEntityList((StorageEntityList list) -> {
                return null;
            });
            storageDataPath = storagePath.resolve("./data");
            if (!Files.exists(storageDataPath)) {
                Files.createDirectories(storageDataPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <R> R withStorageEntityList(Callback<StorageEntityList, R> callback) {
        try (FileChannel fc = FileChannel.open(storageMetaEntitiyListPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            StorageEntityList storageEntityList = new StorageEntityList(fc);
            return callback.call(storageEntityList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <R> R withStorage(String name, Callback<ru.otus.btree.lib.api.storage.IStorage, R> callback) {
        Path dataPath = storageDataPath.resolve(name + ".data");
        Path metaPath = storageDataPath.resolve(name + ".meta");

        try (FileChannel dataFc = FileChannel.open(dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileChannel metaFc = FileChannel.open(metaPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ) {
            ru.otus.btree.lib.v1.storage.Storage storage = new ru.otus.btree.lib.v1.storage.Storage(dataFc, metaFc);
            return callback.call(storage);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <R> R withBTree(String entityName, String entityField, Callback<IBTree, R> callback) {
        Path dataPath = storageDataPath.resolve("index_" + entityName + "_" + entityField + ".data");
        Path metaPath = storageDataPath.resolve("index_" + entityName + "_" + entityField + ".meta");

        try (FileChannel dataFc = FileChannel.open(dataPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileChannel metaFc = FileChannel.open(metaPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ) {
            IBTree btree = new FileBTree(dataFc, metaFc, B_TREE_DEGREE);
            return callback.call(btree);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void createIndex(String entityName, String fieldName) {
        Objects.requireNonNull(entityName, fieldName);
        withStorage(entityName, (storage) -> {
            return withBTree(entityName, fieldName, (btree) -> {
                for (int i = 0; i < storage.size(); i++) {
                    Result result = storage.get(i);
                    IEntity data = result.getData();
                    if (data != null) {
                        btree.insert(fieldName, result.getData());
                    }
                }
                return null;
            });
        });
    }

    @Override
    public IArray<IStorageIndexInfo> getIndexInfo(String entityName) {
        return new SingleArray<>(0);
    }

    @Override
    public void createEntityStorage(String name) {
        withStorage(name, (storage) -> {
            return null;
        });
    }

    @Override
    public void setEntity(String name, IEntity entity) {
        withStorage(name, (storage) -> {
            storage.insert(new SingleArray<>(new IEntity[]{entity}));
            return null;
        });
    }

    @Override
    public IEntity getEntity(String name, int position) {
        Result result = withStorage(name, (storage) -> {
            return storage.get(position);
        });
        return result.getData();
    }

    @Override
    public IArray<String> getEntitiesList() {
        IArray<String> result = withStorageEntityList((list) -> {
            IArray<String> r = new SingleArray<>(0);
            for (int i = 0; i < list.getSize(); i++) {
                r.add(r.size(), list.getEntity(i).getName());
            }
            return r;
        });
        return result;
    }

    @Override
    public IStorageEntityInfo getEntityInfo(String name) {
        return withStorage(name, (storage) -> {
            return new StorageEntityInfo(name, storage.size(), storage.fileSize());
        });
    }
}
