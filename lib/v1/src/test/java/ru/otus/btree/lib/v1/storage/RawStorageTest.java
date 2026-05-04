package ru.otus.btree.lib.v1.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.IEntity;
import ru.otus.btree.lib.v1.btree.Entity;
import ru.otus.btree.lib.v1.btree.FileBTreeUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RawStorageTest {

    @TempDir
    Path tempDir;

    private Path tempFile;
    private FileChannel fileChannel;
    private RawStorage rawStorage;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = tempDir.resolve(UUID.randomUUID().toString() + ".dat");
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        rawStorage = new RawStorage(fileChannel);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
    }

    @Test
    public void testSetAndGetEntity() {
        IEntity original = createEntity();
        original.set(new Element("key1", EType.STRING, "value1"));
        original.set(new Element("key2", EType.INTEGER, 42));

        byte[] data = FileBTreeUtils.serializeEntity(original);
        int size = data.length;

        int writtenSize = rawStorage.set(0L, original);
        assertEquals(size, writtenSize);

        IEntity loaded = rawStorage.get(0L, size);

        assertNotNull(loaded);
        assertEquals("value1", loaded.get("key1").getValue());
        assertEquals(42, loaded.get("key2").getValue());
    }

    @Test
    public void testSetAndGetMultipleEntities() {
        IEntity entity1 = createEntity();
        entity1.set(new Element("name", EType.STRING, "first"));

        IEntity entity2 = createEntity();
        entity2.set(new Element("name", EType.STRING, "second"));

        byte[] data1 = FileBTreeUtils.serializeEntity(entity1);
        byte[] data2 = FileBTreeUtils.serializeEntity(entity2);

        rawStorage.set(0L, entity1);
        rawStorage.set(data1.length, entity2);

        IEntity loaded1 = rawStorage.get(0L, data1.length);
        IEntity loaded2 = rawStorage.get(data1.length, data2.length);

        assertNotNull(loaded1);
        assertNotNull(loaded2);
        assertEquals("first", loaded1.get("name").getValue());
        assertEquals("second", loaded2.get("name").getValue());
    }

    @Test
    public void testGetWithZeroSizeReturnsNull() {
        IEntity result = rawStorage.get(0L, 0);
        assertNull(result);
    }

    @Test
    public void testGetWithNegativeSizeReturnsNull() {
        IEntity result = rawStorage.get(0L, -1);
        assertNull(result);
    }

    @Test
    public void testSetWithNullThrowsException() {
        assertThrows(NullPointerException.class, () -> rawStorage.set(0L, null));
    }

    @Test
    public void testGetAtInvalidPositionReturnsNull() {
        IEntity loaded = rawStorage.get(1000L, 100);
        assertNull(loaded);
    }

    @Test
    public void testSetOverwritesExistingData() {
        IEntity entity1 = createEntity();
        entity1.set(new Element("version", EType.INTEGER, 1));

        IEntity entity2 = createEntity();
        entity2.set(new Element("version", EType.INTEGER, 2));

        byte[] data = FileBTreeUtils.serializeEntity(entity1);

        rawStorage.set(0L, entity1);
        rawStorage.set(0L, entity2);

        IEntity loaded = rawStorage.get(0L, data.length);

        assertNotNull(loaded);
        assertEquals(2, loaded.get("version").getValue());
    }

    private IEntity createEntity() {
        return new Entity();
    }
}
