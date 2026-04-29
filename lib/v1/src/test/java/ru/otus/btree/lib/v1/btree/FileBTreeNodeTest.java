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
        File tempFile = tempDir.resolve("node-parent-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode node = new FileBTreeNode(1L, 3, true, channel, pageManager);

            assertEquals(-1L, node.getParentPageId());

            node.setParentPageId(5L);
            assertEquals(5L, node.getParentPageId());
        }
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
    public void testFindByKeyInEmptyNode() throws Exception {
        File tempFile = tempDir.resolve("node-find-empty-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode node = new FileBTreeNode(0L, 3, true, channel, pageManager);

            Element key = new Element("key1", EType.STRING, "value1");
            assertNull(node.findByKey(key));
        }
    }

    @Test
    public void testInsertAndFindKey() throws Exception {
        File tempFile = tempDir.resolve("node-insert-find-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode node = new FileBTreeNode(0L, 3, true, channel, pageManager);

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
        File tempFile = tempDir.resolve("node-sorted-test.tmp").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode node = new FileBTreeNode(0L, 3, true, channel, pageManager);

            Element keyB = new Element("key", EType.STRING, "B");
            Element keyA = new Element("key", EType.STRING, "A");
            Element keyC = new Element("key", EType.STRING, "C");

            node.insertByKey(keyB);
            node.insertByKey(keyA);
            node.insertByKey(keyC);

            assertEquals(3, node.getKeys().size());
            assertEquals("A", node.getKeys().get(0).getValue());
            assertEquals("B", node.getKeys().get(1).getValue());
            assertEquals("C", node.getKeys().get(2).getValue());
        }
    }

}
