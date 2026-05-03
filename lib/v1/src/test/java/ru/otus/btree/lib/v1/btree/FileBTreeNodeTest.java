package ru.otus.btree.lib.v1.btree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.v1.array.SingleArray;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileBTreeNodeTest {

    private Logger logger = Logger.getLogger(this.getClass().getName());

    @TempDir
    Path tempDir;

    @Test
    public void testConstructorWithValidArguments() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            FileBTreeNode node = new FileBTreeNode(1L, 3, true, nodeChannel, pageManager);

            assertEquals(1L, node.getPageId());
            assertEquals(3, node.getDegree());
            assertTrue(node.isLeaf());
            assertEquals(0, node.getKeys().size());
            assertEquals(0, node.getChildren().size());
            assertEquals(-1L, node.getParentPageId());
        }
    }

    @Test
    public void testConstructorWithNullFileChannel() {
        assertThrows(NullPointerException.class, () -> {
            new FileBTreeNode(1L, 3, true, null, null);
        });
    }

    @Test
    public void testParentPageId() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-parent-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-parent-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            FileBTreeNode node = new FileBTreeNode(1L, 3, true, nodeChannel, pageManager);

            assertEquals(-1L, node.getParentPageId());

            node.setParentPageId(5L);
            assertEquals(5L, node.getParentPageId());
        }
    }

    @Test
    public void testSetLeaf() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-leaf-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-leaf-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            FileBTreeNode node = new FileBTreeNode(1L, 3, true, nodeChannel, pageManager);
            assertTrue(node.isLeaf());

            node.setLeaf(false);
            assertFalse(node.isLeaf());

            node.setLeaf(true);
            assertTrue(node.isLeaf());
        }
    }

    @Test
    public void testAddKeys() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-keys-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-keys-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            FileBTreeNode node = new FileBTreeNode(1L, 3, true, nodeChannel, pageManager);

            Element key1 = new Element("key1", EType.STRING, "value1");
            Element key2 = new Element("key2", EType.INTEGER, 42);

            IArray<Element> bucket1 = new SingleArray<>(0);
            bucket1.add(bucket1.size(), key1);
            node.getKeys().add(node.getKeys().size(), bucket1);

            IArray<Element> bucket2 = new SingleArray<>(0);
            bucket2.add(bucket2.size(), key2);
            node.getKeys().add(node.getKeys().size(), bucket2);

            assertEquals(2, node.getKeys().size());
            assertEquals("key1", node.getKeys().get(0).get(0).getName());
            assertEquals("key2", node.getKeys().get(1).get(0).getName());
        }
    }

    @Test
    public void testAddChildren() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-children-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-children-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            FileBTreeNode node = new FileBTreeNode(1L, 3, false, nodeChannel, pageManager);

            node.getChildren().add(node.getChildren().size(), 100L);
            node.getChildren().add(node.getChildren().size(), 200L);
            node.getChildren().add(node.getChildren().size(), 300L);

            assertEquals(3, node.getChildren().size());
            assertEquals(100L, node.getChildren().get(0));
            assertEquals(200L, node.getChildren().get(1));
            assertEquals(300L, node.getChildren().get(2));
        }
    }

    @Test
    public void testFindByKeyInEmptyNode() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-find-empty-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-find-empty-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            FileBTreeNode node = new FileBTreeNode(0L, 3, true, nodeChannel, pageManager);

            Element key = new Element("key1", EType.STRING, "value1");
            assertNull(node.findByKey(key));
        }
    }

    @Test
    public void testInsertAndFindKey() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-insert-find-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-insert-find-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            FileBTreeNode node = new FileBTreeNode(0L, 3, true, nodeChannel, pageManager);

            Element key1 = new Element("key1", EType.STRING, "value1");
            Element key2 = new Element("key2", EType.STRING, "value2");

            node.insertByKey(key1);
            node.insertByKey(key2);

            assertNotNull(node.findByKey(key1));
            assertNotNull(node.findByKey(key2));

            Element key3 = new Element("key3", EType.STRING, "value3");
            assertNull(node.findByKey(key3));
        }
    }

    @Test
    public void testInsertKeyMaintainsSortedOrder() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-node-sorted-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("b-tree-node-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()
        ) {

            PageManager pageManager = new PageManager(pageChannel);
            long page = pageManager.allocatePage();
            FileBTreeNode node = new FileBTreeNode(page, 4, true, nodeChannel, pageManager);
            FileBTreeNode.saveNode(node, pageChannel);

            Element keyB = new Element("key", EType.STRING, "B");
            Element keyA = new Element("key", EType.STRING, "A");
            Element keyC = new Element("key", EType.STRING, "C");

            node.insertByKey(keyB);
            node.insertByKey(keyA);
            node.insertByKey(keyC);

            assertEquals(3, node.getKeys().size());
            assertEquals("A", node.getKeys().get(0).get(0).getValue());
            assertEquals("B", node.getKeys().get(1).get(0).getValue());
            assertEquals("C", node.getKeys().get(2).get(0).getValue());
        }
    }

    @Test
    public void testSplitNodeCreatesNewRoot() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-split-root-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-split-root-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()
        ) {
            PageManager pageManager = new PageManager(pageChannel);
            long page = pageManager.allocatePage();
            FileBTreeNode node = new FileBTreeNode(page, 3, true, nodeChannel, pageManager);

            Element keyA = new Element("key", EType.STRING, "A");
            Element keyB = new Element("key", EType.STRING, "B");
            Element keyC = new Element("key", EType.STRING, "C");

            node.insertByKey(keyA);
            node.insertByKey(keyB);
            node.insertByKey(keyC);

            // After split a new root should be created at page 0 — load it and verify
            FileBTreeNode root = FileBTreeNode.loadNode(0L, nodeChannel, pageManager, null);
            assertNotNull(root, "Expected new root at page 0 after split");
            assertTrue(root.getChildren().size() > 0, "Root should have children after split");

            // Verify searches from the new root succeed for inserted keys
            assertNotNull(root.findByKey(keyA));
            assertNotNull(root.findByKey(keyB));
            assertNotNull(root.findByKey(keyC));
        }
    }

    @Test
    public void testSplitNodePreservesKeyOrder() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-split-order-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-split-order-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()
        ) {
            PageManager pageManager = new PageManager(pageChannel);
            long page = pageManager.allocatePage();
            AtomicReference<FileBTreeNode> node = new AtomicReference<>();

            node.set(new FileBTreeNode(page, 3, true, nodeChannel, pageManager, (root) -> {
                node.set(root);
            }));

            node.get().insertByKey(new Element("key", EType.STRING, "D"));
            node.get().insertByKey(new Element("key", EType.STRING, "B"));
            node.get().insertByKey(new Element("key", EType.STRING, "A"));
            node.get().insertByKey(new Element("key", EType.STRING, "C"));

            assertNotNull(node.get().findByKey(new Element("key", EType.STRING, "A")));
            assertNotNull(node.get().findByKey(new Element("key", EType.STRING, "B")));
            assertNotNull(node.get().findByKey(new Element("key", EType.STRING, "C")));
            assertNotNull(node.get().findByKey(new Element("key", EType.STRING, "D")));
        }
    }

    @Test
    public void testSplitNodePreservesKeyOrderWithManyElementsElementTypeInteger() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-split-order-many-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-split-order-many-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()
        ) {
            PageManager pageManager = new PageManager(pageChannel);
            long page = pageManager.allocatePage();
            AtomicReference<FileBTreeNode> node = new AtomicReference<>();

            node.set(new FileBTreeNode(page, 3, true, nodeChannel, pageManager, node::set));

            for (int i = 0; i < 20; i++) {
                node.get().insertByKey(new Element("key", EType.INTEGER, i));
            }
            logger.info(node.get().visualize());

            for (int i = 0; i < 20; i++) {
                IArray<Element> found = node.get().findByKey(new Element("key", EType.INTEGER, i));
                assertNotNull(found, "Should find value" + i);
            }
        }
    }

    @Test
    public void testSplitNodeReverseKeyOrderWithManyElementsElementTypeInt() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-split-order-many-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-split-order-many-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()
        ) {
            PageManager pageManager = new PageManager(pageChannel);
            long page = pageManager.allocatePage();
            AtomicReference<FileBTreeNode> node = new AtomicReference<>();

            node.set(new FileBTreeNode(page, 3, true, nodeChannel, pageManager, node::set));

            FileBTreeNode.saveNode(node.get(), nodeChannel);

            for (int i = 12; i >= 0; i--) {
                node.get().insertByKey(new Element("key", EType.INTEGER,  i));
                logger.info(node.get().visualize());
            }
            IArray<Element> found = node.get().findByKey(new Element("key", EType.INTEGER, 3));
            assertNotNull(found, "Should find value" + 3);
//            for (int i = 0; i < 20; i++) {
//            }
        }
    }

    @Test
    public void testSplitNodePreservesKeyOrderWithManyElementsElementTypeIntegerWithRepeatedKeys() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-split-order-many-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-split-order-many-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()
        ) {
            PageManager pageManager = new PageManager(pageChannel);
            long page = pageManager.allocatePage();
            AtomicReference<FileBTreeNode> node = new AtomicReference<>();

            node.set(new FileBTreeNode(page, 3, true, nodeChannel, pageManager, node::set));

            for (int i = 0; i < 1000; i++) {
                node.get().insertByKey(new Element("key", EType.INTEGER, i));
            }

            for (int i = 0; i < 100; i++) {
                node.get().insertByKey(new Element("key", EType.INTEGER, i));
            }

            for (int i = 0; i < 1000; i++) {
                IArray<Element> found = node.get().findByKey(new Element("key", EType.INTEGER, i));
                assertNotNull(found, "Should find value" + i);
            }
        }
    }

    @Test
    public void testInsertDuplicateKeysIntoSameBucket() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-dup-bucket-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-dup-bucket-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            FileBTreeNode node = new FileBTreeNode(0L, 5, true, nodeChannel, pageManager);

            Element dup1 = new Element("key", EType.STRING, "A", 100L);
            Element dup2 = new Element("key", EType.STRING, "A", 200L);
            Element dup3 = new Element("key", EType.STRING, "A", 300L);

            node.insertByKey(dup1);
            node.insertByKey(dup2);
            node.insertByKey(dup3);

            assertEquals(1, node.getKeys().size(), "All duplicates should be stored in a single bucket");
            assertEquals(3, node.getKeys().get(0).size(), "Bucket should contain 3 elements");
            assertEquals(100L, node.getKeys().get(0).get(0).getPosition());
            assertEquals(200L, node.getKeys().get(0).get(1).getPosition());
            assertEquals(300L, node.getKeys().get(0).get(2).getPosition());
        }
    }

    @Test
    public void testVisualize() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-visualize-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-visualize-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            long page = pageManager.allocatePage();
            AtomicReference<FileBTreeNode> node = new AtomicReference<>();

            node.set(new FileBTreeNode(page, 3, true, nodeChannel, pageManager, node::set));

            node.get().insertByKey(new Element("key", EType.STRING, "B"));
            node.get().insertByKey(new Element("key", EType.STRING, "A"));
            node.get().insertByKey(new Element("key", EType.STRING, "C"));

            String visualization = node.get().visualize();
            assertNotNull(visualization);
            assertTrue(visualization.contains("Node[pageId=0"), "Should contain root node");
            assertTrue(visualization.contains("keys: [B]"), "Root should contain key B");
            assertTrue(visualization.contains("leaf=false"), "Root should not be leaf");
            assertTrue(visualization.contains("leaf=true"), "Children should be leaves");
        }
    }

    @Test
    public void testFindParentByTreeSearch() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-parent-search-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-parent-search-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            long page = pageManager.allocatePage();
            AtomicReference<FileBTreeNode> node = new AtomicReference<>();

            node.set(new FileBTreeNode(page, 3, true, nodeChannel, pageManager, node::set));

            node.get().insertByKey(new Element("key", EType.STRING, "B"));
            node.get().insertByKey(new Element("key", EType.STRING, "A"));
            node.get().insertByKey(new Element("key", EType.STRING, "C"));

            FileBTreeNode root = node.get();
            assertNull(root.findParentByTreeSearch(), "Root should have no parent");

            // Root should have two children after split
            assertEquals(2, root.getChildren().size(), "Root should have 2 children");

            // Load each child and verify it finds the root as its parent
            for (int i = 0; i < root.getChildren().size(); i++) {
                Long childPageId = root.getChildren().get(i);
                FileBTreeNode child = FileBTreeNode.loadNode(childPageId, nodeChannel, pageManager, node::set);
                FileBTreeNode parent = child.findParentByTreeSearch();
                assertNotNull(parent, "Child should find a parent");
                assertEquals(root.getPageId(), parent.getPageId(), "Parent should be the root");
            }
        }
    }

    @Test
    public void testBucketSplitWithDuplicates() throws Exception {
        File pageTempFile = tempDir.resolve("page-manager-dup-split-test.tmp").toFile();
        File nodeTempFile = tempDir.resolve("node-dup-split-test.tmp").toFile();
        try (RandomAccessFile pageRaf = new RandomAccessFile(pageTempFile, "rw");
             FileChannel pageChannel = pageRaf.getChannel();
             RandomAccessFile nodeRaf = new RandomAccessFile(nodeTempFile, "rw");
             FileChannel nodeChannel = nodeRaf.getChannel()) {

            PageManager pageManager = new PageManager(pageChannel);
            long page = pageManager.allocatePage();
            AtomicReference<FileBTreeNode> nodeRef = new AtomicReference<>();
            nodeRef.set(new FileBTreeNode(page, 3, true, nodeChannel, pageManager, (root) -> {
                nodeRef.set(root);
            }));

            // degree=3 => max 2 buckets before split
            nodeRef.get().insertByKey(new Element("k", EType.STRING, "A"));
            nodeRef.get().insertByKey(new Element("k", EType.STRING, "B"));
            nodeRef.get().insertByKey(new Element("k", EType.STRING, "C"));

            assertNotNull(nodeRef.get().findByKey(new Element("k", EType.STRING, "A")));
            assertNotNull(nodeRef.get().findByKey(new Element("k", EType.STRING, "B")));
            assertNotNull(nodeRef.get().findByKey(new Element("k", EType.STRING, "C")));
        }
    }
}
