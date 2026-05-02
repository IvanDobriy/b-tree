package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IBTree;
import ru.otus.btree.lib.api.btree.IEntity;

import java.nio.channels.FileChannel;
import java.util.Objects;

public class FileBTree implements IBTree {
    private FileBTreeNode fileBTreeNode;
    private PageManager pageManager;
    private FileChannel pageManagerChannel;
    private FileChannel nodeChannel;
    private final int degree;
    private FileBTreeNode root;


    private final FileBTreeNode.IOnRootChanged onRootChanged = (FileBTreeNode root) -> {
        this.root = root;
    };

    public FileBTree(FileChannel pageManagerChannel, FileChannel nodeChannel, int degree) {
        this.pageManagerChannel = Objects.requireNonNull(pageManagerChannel, "page manager channel is null");
        this.nodeChannel = Objects.requireNonNull(nodeChannel, "node channel is null");
        this.degree = degree;
        pageManager = new PageManager(this.pageManagerChannel);
        root = getRoot();
    }

    @Override
    public void insert(String keyName, IEntity entity, long position) {
        root.insertByKey(entity.get(keyName));
    }

    @Override
    public Element search(Element element) {
        element = root.findByKey(element);
        return element;
    }

    @Override
    public void delete(Element element) {
        // TODO: Implement delete
        throw new RuntimeException("not yet implemented");
    }

    public FileBTreeNode getRoot() {
        if (pageManager.getPageSize() != 0) {//todo need fix, need add real check root existing
            long pageId = pageManager.allocatePage();
            FileBTreeNode newRoot = new FileBTreeNode(pageId, degree, false, nodeChannel, pageManager, onRootChanged);
            FileBTreeNode.saveNode(newRoot, nodeChannel);
            return newRoot;
        }
        return FileBTreeNode.loadNode(0, nodeChannel, pageManager);
    }
}
