package ru.otus.btree.lib.v1.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.IEntity;
import ru.otus.btree.lib.api.storage.Result;
import ru.otus.btree.lib.v1.array.SingleArray;
import ru.otus.btree.lib.v1.btree.Entity;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageTest {

    @TempDir
    Path tempDir;

    private Path dataFile;
    private Path metaFile;
    private FileChannel dataChannel;
    private FileChannel metaChannel;

    @BeforeEach
    public void setUp() throws IOException {
        dataFile = tempDir.resolve(UUID.randomUUID().toString() + "_data.dat");
        metaFile = tempDir.resolve(UUID.randomUUID().toString() + "_meta.dat");
        dataChannel = FileChannel.open(dataFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        metaChannel = FileChannel.open(metaFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (dataChannel != null && dataChannel.isOpen()) {
            dataChannel.close();
        }
        if (metaChannel != null && metaChannel.isOpen()) {
            metaChannel.close();
        }
    }

    @Test
    public void testInsertAndGet() {
        Storage storage = new Storage(dataChannel, metaChannel);

        IArray<IEntity> entities = new SingleArray<>(1);
        Entity entity = new Entity();
        entity.set(new Element("name", EType.STRING, "test_value"));
        entities.add(0, entity);

        storage.insert(entities);
        assertEquals(1, storage.size());

        Element search = new Element("search", EType.STRING, "", 0L);
        Result result = storage.get(search);

        assertNotNull(result);
        assertNotNull(result.getData());
        assertEquals("test_value", result.getData().get("name").getValue());
        assertTrue(result.getPosition() >= 0);
    }

    @Test
    public void testInsertMultipleAndGetRange() {
        Storage storage = new Storage(dataChannel, metaChannel);

        IArray<IEntity> entities = new SingleArray<>(3);
        for (int i = 0; i < 3; i++) {
            Entity entity = new Entity();
            entity.set(new Element("id", EType.INTEGER, i));
            entity.set(new Element("name", EType.STRING, "value" + i));
            entities.add(i, entity);
        }

        storage.insert(entities);
        assertEquals(3, storage.size());

        IArray<Result> results = storage.get(0L, 2L);
        assertEquals(3, results.size());

        for (int i = 0; i < 3; i++) {
            Result result = results.get(i);
            assertNotNull(result);
            assertNotNull(result.getData());
            assertEquals(i, result.getData().get("id").getValue());
            assertEquals("value" + i, result.getData().get("name").getValue());
        }
    }

    @Test
    public void testSizeAfterInsert() {
        Storage storage = new Storage(dataChannel, metaChannel);
        assertEquals(0, storage.size());

        IArray<IEntity> entities = new SingleArray<>(1);
        Entity entity = new Entity();
        entity.set(new Element("key", EType.STRING, "v1"));
        entities.add(0, entity);

        storage.insert(entities);
        assertEquals(1, storage.size());

        storage.insert(entities);
        assertEquals(2, storage.size());
    }

    @Test
    public void testGetNotFound() {
        Storage storage = new Storage(dataChannel, metaChannel);

        Element search = new Element("search", EType.INTEGER, 0, 100L);
        Result result = storage.get(search);

        assertNull(result);
    }

    @Test
    public void testGetRangeEmpty() {
        Storage storage = new Storage(dataChannel, metaChannel);

        IArray<Result> results = storage.get(10L, 20L);

        assertNotNull(results);
        assertEquals(0, results.size());
    }

    @Test
    public void testInsertNullEntityArray() {
        Storage storage = new Storage(dataChannel, metaChannel);
        assertThrows(NullPointerException.class, () -> storage.insert(null));
    }
}
