package ru.otus.btree.lib.v1.btree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IEntity;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileBTreeUtilsTest {

    @TempDir
    Path tempDir;

    @Test
    public void testSerializeDeserializeStringElement() {
        Element original = new Element("testKey", EType.STRING, "testValue");
        byte[] serialized = FileBTreeUtils.serializeElement(original);
        Element deserialized = FileBTreeUtils.deserializeElement(serialized);

        assertNotNull(deserialized);
        assertEquals("testKey", deserialized.getName());
        assertEquals(EType.STRING, deserialized.getType());
        assertEquals("testValue", deserialized.getValue());
    }

    @Test
    public void testSerializeDeserializeIntegerElement() {
        Element original = new Element("intKey", EType.INTEGER, 42);
        byte[] serialized = FileBTreeUtils.serializeElement(original);
        Element deserialized = FileBTreeUtils.deserializeElement(serialized);

        assertNotNull(deserialized);
        assertEquals("intKey", deserialized.getName());
        assertEquals(EType.INTEGER, deserialized.getType());
        assertEquals(42, deserialized.getValue());
    }

    @Test
    public void testSerializeNullElement() {
        byte[] serialized = FileBTreeUtils.serializeElement(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullData() {
        Element result = FileBTreeUtils.deserializeElement(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        Element result = FileBTreeUtils.deserializeElement(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testSerializeDeserializeStringWithNullValue() {
        Element original = new Element("nullStringKey", EType.STRING, null);
        byte[] serialized = FileBTreeUtils.serializeElement(original);
        Element deserialized = FileBTreeUtils.deserializeElement(serialized);

        assertNotNull(deserialized);
        assertEquals("nullStringKey", deserialized.getName());
        assertEquals(EType.STRING, deserialized.getType());
        assertNull(deserialized.getValue());
    }

    @Test
    public void testSerializeDeserializeIntegerWithNullValue() {
        Element original = new Element("nullIntKey", EType.INTEGER, null);
        byte[] serialized = FileBTreeUtils.serializeElement(original);
        Element deserialized = FileBTreeUtils.deserializeElement(serialized);

        assertNotNull(deserialized);
        assertEquals("nullIntKey", deserialized.getName());
        assertEquals(EType.INTEGER, deserialized.getType());
        // null integer is serialized as 0
        assertEquals(0, deserialized.getValue());
    }

    @Test
    public void testSerializeDeserializeUtf8String() {
        Element original = new Element("ключ", EType.STRING, "значение");
        byte[] serialized = FileBTreeUtils.serializeElement(original);
        Element deserialized = FileBTreeUtils.deserializeElement(serialized);

        assertNotNull(deserialized);
        assertEquals("ключ", deserialized.getName());
        assertEquals("значение", deserialized.getValue());
    }

    @Test
    public void testRoundTripMultipleElements() {
        Element[] elements = {
                new Element("key1", EType.STRING, "value1"),
                new Element("key2", EType.INTEGER, 100),
                new Element("key3", EType.STRING, null),
                new Element("key4", EType.INTEGER, -999),
                new Element("key5", EType.STRING, "special!@#$%^&*()")
        };

        for (Element original : elements) {
            byte[] serialized = FileBTreeUtils.serializeElement(original);
            Element deserialized = FileBTreeUtils.deserializeElement(serialized);

            assertNotNull(deserialized);
            assertEquals(original.getName(), deserialized.getName());
            assertEquals(original.getType(), deserialized.getType());
            assertEquals(original.getValue(), deserialized.getValue());
        }
    }

    @Test
    public void testSerializeDeserializeEntity() {
        Entity original = new Entity();
        original.set(new Element("key1", EType.STRING, "value1"));
        original.set(new Element("key2", EType.INTEGER, 42));
        original.set(new Element("key3", EType.STRING, null));

        byte[] serialized = FileBTreeUtils.serializeEntity(original);
        IEntity deserialized = FileBTreeUtils.deserializeEntity(serialized);

        assertNotNull(deserialized);
        assertTrue(deserialized instanceof Entity);

        Entity result = (Entity) deserialized;
        assertEquals(3, result.size());

        Element elem1 = result.get("key1");
        assertNotNull(elem1);
        assertEquals("value1", elem1.getValue());

        Element elem2 = result.get("key2");
        assertNotNull(elem2);
        assertEquals(42, elem2.getValue());

        Element elem3 = result.get("key3");
        assertNotNull(elem3);
        assertNull(elem3.getValue());
    }

    @Test
    public void testSerializeNullEntity() {
        byte[] serialized = FileBTreeUtils.serializeEntity(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullEntityData() {
        IEntity result = FileBTreeUtils.deserializeEntity(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyEntityData() {
        IEntity result = FileBTreeUtils.deserializeEntity(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testSerializeEmptyEntity() {
        Entity original = new Entity();

        byte[] serialized = FileBTreeUtils.serializeEntity(original);
        IEntity deserialized = FileBTreeUtils.deserializeEntity(serialized);

        assertNotNull(deserialized);
        assertTrue(deserialized instanceof Entity);
        assertEquals(0, ((Entity) deserialized).size());
    }

    @Test
    public void testSerializeEntityWithManyElements() {
        Entity original = new Entity();
        for (int i = 0; i < 50; i++) {
            original.set(new Element("key" + i, EType.INTEGER, i));
        }

        byte[] serialized = FileBTreeUtils.serializeEntity(original);
        IEntity deserialized = FileBTreeUtils.deserializeEntity(serialized);

        assertNotNull(deserialized);
        Entity result = (Entity) deserialized;
        assertEquals(50, result.size());

        for (int i = 0; i < 50; i++) {
            Element elem = result.get("key" + i);
            assertNotNull(elem);
            assertEquals(i, elem.getValue());
        }
    }

    @Test
    public void testSerializeDeserializeFileBTreeNode() throws Exception {
        File tempFile = tempDir.resolve("btree-test.tmp").toFile();

        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode original = new FileBTreeNode(1L, 3, true, channel, pageManager);
            original.getKeys().add(original.getKeys().size(), new Element("key1", EType.STRING, "value1"));
            original.getKeys().add(original.getKeys().size(), new Element("key2", EType.INTEGER, 42));
            original.getChildren().add(original.getChildren().size(), 2L);
            original.getChildren().add(original.getChildren().size(), 3L);

            byte[] serialized = FileBTreeUtils.serializeFileBTreeNode(original);
            FileBTreeNode deserialized = FileBTreeUtils.deserializeFileBTreeNode(serialized, channel, pageManager);

            assertNotNull(deserialized);
            assertEquals(1L, deserialized.getPageId());
            assertEquals(3, deserialized.getDegree());
            assertTrue(deserialized.isLeaf());
            // parentPageId is serialized/deserialized; should match the original (default -1)
            assertEquals(original.getParentPageId(), deserialized.getParentPageId());
            assertEquals(2, deserialized.getKeys().size());
            assertEquals(2, deserialized.getChildren().size());

            Element key1 = deserialized.getKeys().get(0);
            assertEquals("key1", key1.getName());
            assertEquals("value1", key1.getValue());

            Element key2 = deserialized.getKeys().get(1);
            assertEquals("key2", key2.getName());
            assertEquals(42, key2.getValue());

            assertEquals(2L, deserialized.getChildren().get(0));
            assertEquals(3L, deserialized.getChildren().get(1));
        }
    }

    @Test
    public void testSerializeNullFileBTreeNode() {
        byte[] serialized = FileBTreeUtils.serializeFileBTreeNode(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullFileBTreeNodeData() throws Exception {
        File tempFile = tempDir.resolve("btree-test-null.tmp").toFile();

        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode result = FileBTreeUtils.deserializeFileBTreeNode(null, channel, pageManager);
            assertNull(result);
        }
    }

    @Test
    public void testDeserializeEmptyFileBTreeNodeData() throws Exception {
        File tempFile = tempDir.resolve("btree-test-empty.tmp").toFile();

        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode result = FileBTreeUtils.deserializeFileBTreeNode(new byte[0], channel, pageManager);
            assertNull(result);
        }
    }

    @Test
    public void testSerializeEmptyFileBTreeNode() throws Exception {
        File tempFile = tempDir.resolve("btree-test-empty-node.tmp").toFile();

        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
             FileChannel channel = raf.getChannel()) {

            PageManager pageManager = new PageManager(channel);
            FileBTreeNode original = new FileBTreeNode(1L, 3, false, channel, pageManager);

            byte[] serialized = FileBTreeUtils.serializeFileBTreeNode(original);
            FileBTreeNode deserialized = FileBTreeUtils.deserializeFileBTreeNode(serialized, channel, pageManager);

            assertNotNull(deserialized);
            assertEquals(1L, deserialized.getPageId());
            assertEquals(3, deserialized.getDegree());
            assertFalse(deserialized.isLeaf());
            // parentPageId is serialized/deserialized; should match the original (default -1)
            assertEquals(original.getParentPageId(), deserialized.getParentPageId());
            assertEquals(0, deserialized.getKeys().size());
            assertEquals(0, deserialized.getChildren().size());
        }
    }
}
