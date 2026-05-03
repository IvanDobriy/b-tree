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
    private long pageId;
    private final int degree;
    private final IArray<IArray<Element>> keys;
    private final IArray<Long> children;
    private boolean isLeaf;
    private FileChannel fileChannel;
    private PageManager pageManager; // Page allocation manager
    private IOnRootChanged onRootChanged;


    public interface IOnRootChanged {
        void execute(FileBTreeNode root);
    }

    public static FileBTreeNode loadNode(long pageId, FileChannel fileChannel, PageManager pageManager, IOnRootChanged onRootChanged) {
        Objects.requireNonNull(fileChannel, "fileChannel must not be null");
        Objects.requireNonNull(pageManager, "pageManager must not be null");

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
            FileBTreeNode node = FileBTreeUtils.deserializeFileBTreeNode(nodeData, fileChannel, pageManager);
            if (node != null && onRootChanged != null) {
                node.onRootChanged = onRootChanged;
            }
            return node;
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
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
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
        this.onRootChanged = null;
    }

    public FileBTreeNode(long pageId, int degree, boolean isLeaf, FileChannel fileChannel, PageManager pageManager, IOnRootChanged onRootChanged) {
        this(pageId, degree, isLeaf, fileChannel, pageManager);
        this.onRootChanged = onRootChanged;
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

    public int getDegree() {
        return degree;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public IArray<IArray<Element>> getKeys() {
        return keys;
    }

    public IArray<Long> getChildren() {
        return children;
    }

    public String visualize() {
        StringBuilder sb = new StringBuilder();
        visualize(sb, 0);
        return sb.toString();
    }

    private void visualize(StringBuilder sb, int depth) {
        String indent = "  ".repeat(depth);
        sb.append(indent).append("Node[pageId=").append(pageId)
          .append(", leaf=").append(isLeaf).append("]\n");

        sb.append(indent).append("  keys: [");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(bucketToString(keys.get(i)));
        }
        sb.append("]\n");

        if (!isLeaf && children.size() > 0) {
            sb.append(indent).append("  children:\n");
            for (int i = 0; i < children.size(); i++) {
                Long childPageId = children.get(i);
                if (childPageId != null) {
                    FileBTreeNode child = loadNode(childPageId, fileChannel, pageManager, onRootChanged);
                    child.visualize(sb, depth + 1);
                }
            }
        }
    }

    private String bucketToString(IArray<Element> bucket) {
        if (bucket == null || bucket.size() == 0) {
            return "null";
        }
        Element first = bucket.get(0);
        String valueStr = first.getValue() != null ? first.getValue().toString() : "null";
        if (bucket.size() > 1) {
            return valueStr + "(×" + bucket.size() + ")";
        }
        return valueStr;
    }

    /**
     * Finds the parent of this node by traversing the tree from the root.
     *
     * @return the parent node, or null if this node is the root
     */
    public FileBTreeNode findParentByTreeSearch() {
        if (this.pageId == 0) {
            return null; // Root is always at page 0
        }
        FileBTreeNode root = loadNode(0, fileChannel, pageManager, onRootChanged);
        return findParentInSubtree(root, this.pageId);
    }

    private FileBTreeNode findParentInSubtree(FileBTreeNode node, long targetPageId) {
        if (node == null || node.isLeaf()) {
            return null;
        }
        // Check if the target is a direct child of this node
        for (int i = 0; i < node.children.size(); i++) {
            Long childPageId = node.children.get(i);
            if (childPageId != null && childPageId == targetPageId) {
                return node;
            }
        }
        // Recurse into each child subtree
        for (int i = 0; i < node.children.size(); i++) {
            Long childPageId = node.children.get(i);
            if (childPageId != null) {
                FileBTreeNode child = loadNode(childPageId, fileChannel, pageManager, onRootChanged);
                FileBTreeNode found = findParentInSubtree(child, targetPageId);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    public IArray<Element> findByKey(Element key) {
        if (key == null) {
            return null;
        }

        // Search in current node's keys by comparing values based on type
        for (int i = 0; i < keys.size(); i++) {
            IArray<Element> bucket = keys.get(i);
            if (bucket != null && bucket.size() > 0 && compareElements(bucket.get(0), key) == 0) {
                return bucket;
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
        FileBTreeNode childNode = loadNode(childPageId, fileChannel, pageManager, this.onRootChanged);
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
            if (keys.size() > degree - 1) {
                splitNode();
            } else {
                saveNode(this, fileChannel);
            }
        } else {
            // Find the appropriate child to insert into
            int childIndex = findChildIndex(key);

            if (childIndex < children.size()) {
                Long childPageId = children.get(childIndex);
                FileBTreeNode childNode = loadNode(childPageId, fileChannel, pageManager, this.onRootChanged);
                childNode.insertByKey(key);
            } else {
                // If no valid child, insert into current node
                insertKeyIntoNode(key);

                if (keys.size() > degree - 1) {
                    splitNode();
                } else {
                    saveNode(this, fileChannel);
                }
            }
        }
    }

    public void deleteByKey(Element key) {
        Objects.requireNonNull(key, "key must not be null");
        if (isLeaf) {
            removeKeyFromNode(key);
            saveNode(this, fileChannel);
        } else {
            int keyIndex = -1;
            for (int i = 0; i < keys.size(); i++) {
                IArray<Element> bucket = keys.get(i);
                if (bucket != null && bucket.size() > 0 && compareElements(bucket.get(0), key) == 0) {
                    keyIndex = i;
                    break;
                }
            }
            if (keyIndex >= 0) {
                // Replace with predecessor (max key from left child subtree)
                Long leftChildPageId = children.get(keyIndex);
                FileBTreeNode predecessorNode = loadNode(leftChildPageId, fileChannel, pageManager, this.onRootChanged);
                while (!predecessorNode.isLeaf()) {
                    int lastChildIndex = predecessorNode.getChildren().size() - 1;
                    Long lastChildPageId = predecessorNode.getChildren().get(lastChildIndex);
                    predecessorNode = loadNode(lastChildPageId, fileChannel, pageManager, this.onRootChanged);
                }
                IArray<Element> predecessorBucket = predecessorNode.getKeys().get(predecessorNode.getKeys().size() - 1);
                // Copy bucket to avoid shared reference
                IArray<Element> newBucket = new SingleArray<>(0);
                for (int i = 0; i < predecessorBucket.size(); i++) {
                    newBucket.add(i, predecessorBucket.get(i));
                }
                keys.set(keyIndex, newBucket);
                saveNode(this, fileChannel);
                // Remove predecessor from leaf
                predecessorNode.removeKeyFromNode(predecessorBucket.get(0));
                saveNode(predecessorNode, fileChannel);
            } else {
                int childIndex = findChildIndex(key);
                if (childIndex < children.size()) {
                    Long childPageId = children.get(childIndex);
                    FileBTreeNode childNode = loadNode(childPageId, fileChannel, pageManager, this.onRootChanged);
                    childNode.deleteByKey(key);
                }
            }
        }
    }

    private int findChildIndex(Element key) {
        int childIndex = 0;
        for (int i = 0; i < keys.size(); i++) {
            IArray<Element> bucket = keys.get(i);
            if (bucket != null && bucket.size() > 0 && compareElements(key, bucket.get(0)) < 0) {
                childIndex = i;
                break;
            }
            childIndex = i + 1;
        }
        return childIndex;
    }

    private int removeKeyFromNode(Element key) {
        for (int i = 0; i < keys.size(); i++) {
            IArray<Element> bucket = keys.get(i);
            if (bucket != null && bucket.size() > 0 && compareElements(key, bucket.get(0)) == 0) {
                for (int j = 0; j < bucket.size(); j++) {
                    if (compareElements(bucket.get(j), key) == 0) {
                        bucket.remove(j);
                        break;
                    }
                }
                if (bucket.size() == 0) {
                    keys.remove(i);
                }
                return i;
            }
        }
        return -1; // Key not found
    }

    private int insertKeyIntoNode(Element key) {
        // Check if a bucket with the same key already exists
        for (int i = 0; i < keys.size(); i++) {
            IArray<Element> bucket = keys.get(i);
            if (bucket != null && bucket.size() > 0 && compareElements(key, bucket.get(0)) == 0) {
                bucket.add(bucket.size(), key);
                return i;
            }
        }

        // Create a new bucket for this key
        IArray<Element> bucket = new SingleArray<>(0);
        bucket.add(0, key);
        return insertBucketIntoNode(bucket);
    }

    private int insertBucketIntoNode(IArray<Element> bucket) {
        // Find the correct position by comparing from the end
        // Shift buckets to the right to make room for the new bucket
        int insertIndex = keys.size();
        for (int i = keys.size() - 1; i >= 0; i--) {
            IArray<Element> currentBucket = keys.get(i);
            if (currentBucket != null && currentBucket.size() > 0 && compareElements(bucket.get(0), currentBucket.get(0)) < 0) {
                // Shift current bucket to the right
                if (i + 1 < keys.size()) {
                    keys.set(i + 1, currentBucket);
                } else {
                    keys.add(keys.size(), currentBucket);
                }
                insertIndex = i;
            } else {
                break;
            }
        }
        // Insert the new bucket at the found position
        if (insertIndex < keys.size()) {
            keys.set(insertIndex, bucket);
        } else {
            keys.add(insertIndex, bucket);
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
        IArray<Element> medianBucket = keys.get(medianIndex);

        // Create new right sibling node using PageManager to allocate page
        long newPageId = pageManager.allocatePage();
        FileBTreeNode rightSibling = new FileBTreeNode(newPageId, degree, isLeaf, fileChannel, pageManager, this.onRootChanged);

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
        saveNode(this, fileChannel);
        // Promote medianBucket to parent (or create new root if this is root)
        promoteToParent(medianBucket, newPageId);
    }

    private void promoteToParent(IArray<Element> medianBucket, long rightSiblingPageId) {
        FileBTreeNode parent = this.findParentByTreeSearch();
        if (parent == null) {
            // This is the root node, create a new root
            long newRootPageId = 0; // Root is always at page 0
            FileBTreeNode newRoot = new FileBTreeNode(newRootPageId, degree, false, fileChannel, pageManager, this.onRootChanged);
            newRoot.getKeys().add(0, medianBucket);

            // Current node needs a new pageId since root now occupies page 0
            long newPageId = pageManager.allocatePage();
            newRoot.getChildren().add(0, newPageId);
            newRoot.getChildren().add(1, rightSiblingPageId);

            // Update pageId
            this.pageId = newPageId;
            saveNode(this, fileChannel);

            // Save the new root
            saveNode(newRoot, fileChannel);
            if (onRootChanged != null) {
                onRootChanged.execute(newRoot);
            }
        } else {
            // Insert median bucket into parent using insertBucketIntoNode
            int keyInsertIndex = parent.insertBucketIntoNode(medianBucket);

            // Add right sibling as child after the inserted key
            int childInsertIndex = keyInsertIndex + 1;
            parent.getChildren().add(childInsertIndex, rightSiblingPageId);

            // Save parent
            saveNode(parent, fileChannel);

            if (parent.getPageId() == 0 && onRootChanged != null) {
                onRootChanged.execute(parent);
            }

            // Recursively split parent if needed
            if (parent.getKeys().size() > degree - 1) {
                parent.splitNode();
            }
        }
    }
}
