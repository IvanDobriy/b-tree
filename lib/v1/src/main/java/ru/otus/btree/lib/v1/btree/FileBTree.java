package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IBTree;
import ru.otus.btree.lib.api.btree.IEntity;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

public class FileBTree implements IBTree {
    private final Path entityName;
    private FileChannel fileChannel;

    public FileBTree(Path entityName) {
        this.entityName = Objects.requireNonNull(entityName, "entityName must not be null");
    }

    @Override
    public void insert(String keyName, IEntity entity) {
        // TODO: Implement insert
    }

    @Override
    public IEntity search(Element element) {
        // TODO: Implement search
        return null;
    }

    @Override
    public void delete(Element element) {
        // TODO: Implement delete
    }

}
