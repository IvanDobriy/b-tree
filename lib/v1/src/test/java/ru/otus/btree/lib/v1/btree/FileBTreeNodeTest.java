package ru.otus.btree.lib.v1.btree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.EType;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileBTreeNodeTest {

    @TempDir
    Path tempDir;

    @Test
    public void testConstructorWithValidArguments() throws Exception {
        File tempFile = tempDir.resolve("node-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode node = new FileBTreeNode(1L, 3, true, channel, pageManager);

            assertEquals(1L, node.getPageId());
            assertEquals(3, node.getDegree());
            assertTrue(node.isLeaf());
            assertEquals(0, node.getKeys().size());
            assertEquals(0, node.getChildren().size());
        }
    }

    @Test
    public void testConstructorWithNullFileChannel() {
        assertThrows(NullPointerException.class, () -> {
            new FileBTreeNode(1L, 3, true, null, null);
        });
    }

    @Test
    public void testSetLeaf() throws Exception {
        File tempFile = tempDir.resolve("node-leaf-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode node = new FileBTreeNode(1L, 3, true, channel, pageManager);
            assertTrue(node.isLeaf());

            node.setLeaf(false);
            assertFalse(node.isLeaf());

            node.setLeaf(true);
            assertTrue(node.isLeaf());
        }
    }

    @Test
    public void testAddKeys() throws Exception {
        File tempFile = tempDir.resolve("node-keys-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode node = new FileBTreeNode(1L, 3, true, channel, pageManager);

            Element key1 = new Element("key1", EType.STRING, "value1");
            Element key2 = new Element("key2", EType.INTEGER, 42);

            node.getKeys().add(node.getKeys().size(), key1);
            node.getKeys().add(node.getKeys().size(), key2);

            assertEquals(2, node.getKeys().size());
            assertEquals("key1", node.getKeys().get(0).getName());
            assertEquals("key2", node.getKeys().get(1).getName());
        }
    }

    @Test
    public void testAddChildren() throws Exception {
        File tempFile = tempDir.resolve("node-children-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode node = new FileBTreeNode(1L, 3, false, channel, pageManager);

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
    public void testSaveAndLoadNode() throws Exception {
        File tempFile = tempDir.resolve("node-save-load-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            // Create and save node
            PageManager pageManager = new PageManager(channel);
            FileBTreeNode original = new FileBTreeNode(0L, 3, true, channel, pageManager);
            original.getKeys().add(original.getKeys().size(), new Element("key1", EType.STRING, "value1"));
            original.getKeys().add(original.getKeys().size(), new Element("key2", EType.INTEGER, 100));
            original.getChildren().add(original.getChildren().size(), 1L);
            original.getChildren().add(original.getChildren().size(), 2L);

            FileBTreeNode.saveNode(original, channel);

            // Load the saved node
            FileBTreeNode loaded = FileBTreeNode.loadNode(0L, channel, pageManager);

            assertNotNull(loaded);
            assertEquals(0L, loaded.getPageId());
            assertEquals(3, loaded.getDegree());
            assertTrue(loaded.isLeaf());
            assertEquals(2, loaded.getKeys().size());
            assertEquals(2, loaded.getChildren().size());

            assertEquals("key1", loaded.getKeys().get(0).getName());
            assertEquals("value1", loaded.getKeys().get(0).getValue());
            assertEquals("key2", loaded.getKeys().get(1).getName());
            assertEquals(100, loaded.getKeys().get(1).getValue());

            assertEquals(1L, loaded.getChildren().get(0));
            assertEquals(2L, loaded.getChildren().get(1));
        }
    }

    @Test
    public void testLoadNodeWithNullFileChannel() {
        assertThrows(NullPointerException.class, () -> {
            FileBTreeNode.loadNode(0L, null, null);
        });
    }

    @Test
    public void testSaveNodeWithNullFileChannel() throws Exception {
        File tempFile = tempDir.resolve("node-save-null-channel-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode node = new FileBTreeNode(0L, 3, true, channel, pageManager);

            assertThrows(NullPointerException.class, () -> {
                FileBTreeNode.saveNode(node, null);
            });
        }
    }

    @Test
    public void testSaveNodeWithNullNode() throws Exception {
        File tempFile = tempDir.resolve("node-save-null-node-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            assertThrows(NullPointerException.class, () -> {
                FileBTreeNode.saveNode(null, channel);
            });
        }
    }

    @Test
    public void testEmptyNodeSaveAndLoad() throws Exception {
        File tempFile = tempDir.resolve("node-empty-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode original = new FileBTreeNode(0L, 3, false, channel, pageManager);
            FileBTreeNode.saveNode(original, channel);

            FileBTreeNode loaded = FileBTreeNode.loadNode(0L, channel, pageManager);

            assertNotNull(loaded);
            assertEquals(0, loaded.getKeys().size());
            assertEquals(0, loaded.getChildren().size());
            assertFalse(loaded.isLeaf());
        }
    }
}
