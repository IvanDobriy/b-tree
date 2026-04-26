package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.v1.array.SingleArray;

public class FileBTreeNode {
    private final long pageId;
    private final int degree;
    private final IArray<Element> keys;
    private final IArray<Long> children;
    private boolean isLeaf;

    public FileBTreeNode(long pageId, int degree, boolean isLeaf) {
        this.pageId = pageId;
        this.degree = degree;
        this.isLeaf = isLeaf;
        this.keys = new SingleArray<>(0);
        this.children = new SingleArray<>(0);
    }

    public long getPageId() {
        return pageId;
    }

    public int getDegree() {
        return degree;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public IArray<Element> getKeys() {
        return keys;
    }

    public IArray<Long> getChildren() {
        return children;
    }
}
