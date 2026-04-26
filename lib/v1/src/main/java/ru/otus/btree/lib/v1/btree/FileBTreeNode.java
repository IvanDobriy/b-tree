package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.v1.array.SingleArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class FileBTreeNode {
    private final long pageId;
    private final int degree;
    private final IArray<Element> keys;
    private final IArray<Long> children;
    private boolean isLeaf;
    private FileChannel fileChannel;


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

    public static void saveNode(FileBTreeNode node, long pageId, FileChannel fileChannel) {
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

    public FileBTreeNode(long pageId, int degree, boolean isLeaf, FileChannel fileChannel) {
        this.fileChannel = Objects.requireNonNull(fileChannel, "fileChannel must not be null");
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

    public Element findByKey(Element key) {
        if (key == null) {
            return null;
        }

        String keyName = key.getName();

        // Search in current node's keys
        for (int i = 0; i < keys.size(); i++) {
            Element currentKey = keys.get(i);
            if (currentKey != null && currentKey.getName().equals(keyName)) {
                return currentKey;
            }
        }

        // If this is a leaf, key not found
        if (isLeaf) {
            return null;
        }

        // Determine which child to go to based on key comparison
        int childIndex = 0;
        for (int i = 0; i < keys.size(); i++) {
            Element currentKey = keys.get(i);
            if (currentKey != null && keyName.compareTo(currentKey.getName()) < 0) {
                childIndex = i;
                break;
            }
            childIndex = i + 1;
        }

        // Check if we have a valid child to go to
        if (childIndex >= children.size()) {
            return null;
        }

        // Load the child node and search recursively
        Long childPageId = children.get(childIndex);
        FileBTreeNode childNode = loadNode(childPageId, fileChannel);
        return childNode.findByKey(key);
    }
}
