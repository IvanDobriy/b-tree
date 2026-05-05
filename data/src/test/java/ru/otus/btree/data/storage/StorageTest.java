package ru.otus.btree.data.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import ru.otus.btree.domain.IStorage;
import ru.otus.btree.domain.IStorageEntityInfo;
import ru.otus.btree.domain.IStorageIndexInfo;
import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.IEntity;
import ru.otus.btree.lib.api.storage.Result;
import ru.otus.btree.lib.v1.array.SingleArray;
import ru.otus.btree.lib.v1.btree.Entity;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageTest {

    @TempDir
    Path tempDir;

    private IStorage storage;

    @BeforeEach
    public void setUp() {
        storage = new Storage(tempDir);
    }

    @Test
    public void testCreateEntityStorage() {
        assertDoesNotThrow(() -> storage.createEntityStorage("users"));
    }

    @Test
    public void testSetEntityAndGetEntity() {
        storage.createEntityStorage("users");

        Entity entity = new Entity();
        entity.set(new Element("name", EType.STRING, "Alice"));
        entity.set(new Element("age", EType.INTEGER, 30));

        storage.setEntity("users", entity);

        IEntity loaded = storage.getEntity("users", 0);
        assertNotNull(loaded);
        assertEquals("Alice", loaded.get("name").getValue());
        assertEquals(30, loaded.get("age").getValue());
    }

    @Test
    public void testSetEntityMultipleAndGetEntity() {
        storage.createEntityStorage("items");

        for (int i = 0; i < 5; i++) {
            Entity entity = new Entity();
            entity.set(new Element("id", EType.INTEGER, i));
            entity.set(new Element("value", EType.STRING, "val" + i));
            storage.setEntity("items", entity);
        }

        for (int i = 0; i < 5; i++) {
            IEntity loaded = storage.getEntity("items", i);
            assertNotNull(loaded);
            assertEquals(i, loaded.get("id").getValue());
            assertEquals("val" + i, loaded.get("value").getValue());
        }
    }

    @Test
    public void testGetEntityNotFoundThrowsNpe() {
        storage.createEntityStorage("empty");
        assertThrows(NullPointerException.class, () -> storage.getEntity("empty", 999));
    }

    @Test
    public void testGetEntityInfo() {
        storage.createEntityStorage("metrics");

        Entity entity = new Entity();
        entity.set(new Element("key", EType.STRING, "v1"));
        storage.setEntity("metrics", entity);

        IStorageEntityInfo info = storage.getEntityInfo("metrics");
        assertNotNull(info);
        assertEquals("metrics", info.getName());
        assertEquals(1, info.getSize());
        assertTrue(info.getFileSize() > 0);
    }

    @Test
    public void testGetEntitiesListInitiallyEmpty() {
        IArray<String> entities = storage.getEntitiesList();
        assertNotNull(entities);
        assertEquals(0, entities.size());
    }

    @Test
    public void testCreateIndexDoesNotThrow() {
        storage.createEntityStorage("users");
        assertDoesNotThrow(() -> storage.createIndex("users", "name"));
    }

    @Test
    public void testCreateIndexOnEmptyStorage() {
        storage.createEntityStorage("empty");
        assertDoesNotThrow(() -> storage.createIndex("empty", "field"));
    }

    @Test
    public void testFindByIndexReturnsNull() {
        storage.createEntityStorage("users");
        storage.createIndex("users", "name");

        Element search = new Element("name", EType.STRING, "Alice");
        IArray<Result> result = storage.findByIndex("users", search);
        assertNull(result);
    }

    @Test
    public void testGetIndexInfoReturnsEmpty() {
        IArray<IStorageIndexInfo> info = storage.getIndexInfo("users");
        assertNotNull(info);
        assertEquals(0, info.size());
    }

    @Test
    public void testSetEntityNullDoesNotThrow() {
        storage.createEntityStorage("users");
        assertDoesNotThrow(() -> storage.setEntity("users", null));
    }

    @Test
    public void testCreateEntityStorageTwice() {
        assertDoesNotThrow(() -> storage.createEntityStorage("dup"));
        assertDoesNotThrow(() -> storage.createEntityStorage("dup"));
    }

    @Test
    public void testEntityInfoForEmptyStorage() {
        storage.createEntityStorage("empty");
        IStorageEntityInfo info = storage.getEntityInfo("empty");
        assertNotNull(info);
        assertEquals("empty", info.getName());
        assertEquals(0, info.getSize());
        assertTrue(info.getFileSize() >= 0);
    }
}
