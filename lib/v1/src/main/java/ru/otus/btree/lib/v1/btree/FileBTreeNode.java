package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.v1.array.SingleArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class FileBTreeNode {
    private static final int PAGE_SIZE = 4096; // Standard file system page size

    private long pageId;
    private final int degree;
    private final IArray<Element> keys;
    private final IArray<Long> children;
    private boolean isLeaf;
    private FileChannel fileChannel;
    private long parentPageId; // -1 means no parent (root node)
    private PageManager pageManager; // Page allocation manager


    public static FileBTreeNode loadNode(long pageId, FileChannel fileChannel) {
        Objects.requireNonNull(fileChannel, "fileChannel must not be null");

        try {
            // Read node size first (4 bytes int)
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            fileChannel.position(pageId);
            fileChannel.read(sizeBuffer);
            sizeBuffer.flip();
            int nodeSize = sizeBuffer.getInt();

            // Read node data
            ByteBuffer dataBuffer = ByteBuffer.allocate(nodeSize);
            fileChannel.read(dataBuffer);
            dataBuffer.flip();
            byte[] nodeData = new byte[nodeSize];
            dataBuffer.get(nodeData);

            // Deserialize node
            return FileBTreeUtils.deserializeFileBTreeNode(nodeData, fileChannel);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load node at pageId: " + pageId, e);
        }
    }

    public static void saveNode(FileBTreeNode node, FileChannel fileChannel) {
        Objects.requireNonNull(fileChannel, "fileChannel must not be null");
        Objects.requireNonNull(node, "node must not be null");

        try {
            // Serialize node
            byte[] nodeData = FileBTreeUtils.serializeFileBTreeNode(node);

            // Write node size (4 bytes int) followed by data
            ByteBuffer buffer = ByteBuffer.allocate(4 + nodeData.length);
            buffer.putInt(nodeData.length);
            buffer.put(nodeData);
            buffer.flip();

            fileChannel.position(node.pageId);
            fileChannel.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save node at pageId: " + node.pageId, e);
        }
    }

    public FileBTreeNode(long pageId, int degree, boolean isLeaf, FileChannel fileChannel, PageManager pageManager) {
        this.fileChannel = Objects.requireNonNull(fileChannel, "fileChannel must not be null");
        this.pageManager = Objects.requireNonNull(pageManager, "pageManager must not be null");
        this.pageId = pageId;
        this.degree = degree;
        this.isLeaf = isLeaf;
        this.keys = new SingleArray<>(0);
        this.children = new SingleArray<>(0);
        this.parentPageId = -1; // No parent by default (root node)
    }

    public void setPageManager(PageManager pageManager) {
        this.pageManager = pageManager;
    }

    public PageManager getPageManager() {
        return pageManager;
    }

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public long getParentPageId() {
        return parentPageId;
    }

    public void setParentPageId(long parentPageId) {
        this.parentPageId = parentPageId;
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

    public Element findByKey(Element key) {
        if (key == null) {
            return null;
        }

        // Search in current node's keys by comparing values based on type
        for (int i = 0; i < keys.size(); i++) {
            Element currentKey = keys.get(i);
            if (currentKey != null && compareElements(currentKey, key) == 0) {
                return currentKey;
            }
        }

        // If this is a leaf, key not found
        if (isLeaf) {
            return null;
        }

        // Determine which child to go to based on key comparison
        int childIndex = findChildIndex(key);

        // Check if we have a valid child to go to
        if (childIndex >= children.size()) {
            return null;
        }

        // Load the child node and search recursively
        Long childPageId = children.get(childIndex);
        FileBTreeNode childNode = loadNode(childPageId, fileChannel);
        return childNode.findByKey(key);
    }

    private int compareElements(Element a, Element b) {
        if (a == null || b == null) {
            return a == b ? 0 : (a == null ? -1 : 1);
        }

        // Compare by type first
        if (a.getType() != b.getType()) {
            throw new IllegalArgumentException("Cannot compare elements of different types: " +
                a.getType() + " and " + b.getType());
        }

        // Compare by value based on type
        Object valueA = a.getValue();
        Object valueB = b.getValue();

        if (valueA == null || valueB == null) {
            return valueA == valueB ? 0 : (valueA == null ? -1 : 1);
        }

        if (a.getType() == EType.STRING) {
            String strA = (String) valueA;
            String strB = (String) valueB;
            return strA.compareTo(strB);
        } else if (a.getType() == EType.INTEGER) {
            Integer intA = (Integer) valueA;
            Integer intB = (Integer) valueB;
            return intA.compareTo(intB);
        } else {
            throw new IllegalArgumentException("Unsupported EType: " + a.getType());
        }
    }

    public void insertByKey(Element key) {
        Objects.requireNonNull(key, "key must not be null");

        // If this is a leaf, insert directly
        if (isLeaf) {
            insertKeyIntoNode(key);

            // Check if rebalancing is needed after insertion
            if (keys.size() > degree) {
                splitNode();
            }
        } else {
            // Find the appropriate child to insert into
            int childIndex = findChildIndex(key);

            if (childIndex < children.size()) {
                Long childPageId = children.get(childIndex);
                FileBTreeNode childNode = loadNode(childPageId, fileChannel);
                childNode.insertByKey(key);
                saveNode(childNode, fileChannel);
            } else {
                // If no valid child, insert into current node
                insertKeyIntoNode(key);

                if (keys.size() > degree) {
                    splitNode();
                }
            }
        }
    }

    private int findChildIndex(Element key) {
        int childIndex = 0;
        for (int i = 0; i < keys.size(); i++) {
            Element currentKey = keys.get(i);
            if (currentKey != null && compareElements(key, currentKey) < 0) {
                childIndex = i;
                break;
            }
            childIndex = i + 1;
        }
        return childIndex;
    }

    private int insertKeyIntoNode(Element key) {
        // Find the correct position by comparing from the end
        // Shift elements to the right to make room for the new key
        int insertIndex = keys.size();
        for (int i = keys.size() - 1; i >= 0; i--) {
            Element currentKey = keys.get(i);
            if (currentKey != null && compareElements(key, currentKey) < 0) {
                // Shift current element to the right
                if (i + 1 < keys.size()) {
                    keys.set(i + 1, currentKey);
                } else {
                    keys.add(keys.size(), currentKey);
                }
                insertIndex = i;
            } else {
                break;
            }
        }
        // Insert the new key at the found position
        if (insertIndex < keys.size()) {
            keys.set(insertIndex, key);
        } else {
            keys.add(insertIndex, key);
        }
        return insertIndex;
    }

    private void splitNode() {
        int maxKeys = degree - 1;
        if (keys.size() <= maxKeys) {
            return; // No split needed
        }

        // Find median index
        int medianIndex = keys.size() / 2;
        Element medianKey = keys.get(medianIndex);

        // Create new right sibling node using PageManager to allocate page
        long newPageId = pageManager.allocatePage();
        FileBTreeNode rightSibling = new FileBTreeNode(newPageId, degree, isLeaf, fileChannel, pageManager);
        rightSibling.setParentPageId(parentPageId);

        // Move keys after median to right sibling
        for (int i = medianIndex + 1; i < keys.size(); i++) {
            rightSibling.getKeys().add(rightSibling.getKeys().size(), keys.get(i));
        }

        // Move children after median to right sibling (if not leaf)
        if (!isLeaf) {
            int childSplitIndex = medianIndex + 1;
            for (int i = childSplitIndex; i < children.size(); i++) {
                rightSibling.getChildren().add(rightSibling.getChildren().size(), children.get(i));
            }
            // Remove moved children from this node
            for (int i = children.size() - 1; i >= childSplitIndex; i--) {
                children.remove(i);
            }
        }

        // Remove keys that were moved to right sibling from this node
        for (int i = keys.size() - 1; i >= medianIndex; i--) {
            keys.remove(i);
        }

        // Save the right sibling
        saveNode(rightSibling, fileChannel);

        // Promote medianKey to parent (or create new root if this is root)
        promoteToParent(medianKey, newPageId);

        // Save this node
        saveNode(this, fileChannel);
    }

    private void promoteToParent(Element medianKey, long rightSiblingPageId) {
        if (parentPageId == -1) {
            // This is the root node, create a new root
            long newRootPageId = 0; // Root is always at page 0
            FileBTreeNode newRoot = new FileBTreeNode(newRootPageId, degree, false, fileChannel, pageManager);
            newRoot.getKeys().add(0, medianKey);

            // Current node needs a new pageId since root now occupies page 0
            long newPageId = pageManager.allocatePage();
            newRoot.getChildren().add(0, newPageId);
            newRoot.getChildren().add(1, rightSiblingPageId);

            // Update parent references and pageId
            this.parentPageId = newRootPageId;
            this.pageId = newPageId;
            saveNode(this, fileChannel);

            // Save right sibling with updated parent
            FileBTreeNode rightSibling = loadNode(rightSiblingPageId, fileChannel);
            rightSibling.setParentPageId(newRootPageId);
            saveNode(rightSibling, fileChannel);

            // Save the new root
            saveNode(newRoot, fileChannel);
        } else {
            // Load parent and insert median key
            FileBTreeNode parent = loadNode(parentPageId, fileChannel);

            // Insert median key into parent using insertKeyIntoNode
            int keyInsertIndex = parent.insertKeyIntoNode(medianKey);

            // Add right sibling as child after the inserted key
            int childInsertIndex = keyInsertIndex + 1;
            parent.getChildren().add(childInsertIndex, rightSiblingPageId);

            // Save parent
            saveNode(parent, fileChannel);

            // Recursively split parent if needed
            if (parent.getKeys().size() > degree - 1) {
                parent.splitNode();
            }
        }
    }
}
