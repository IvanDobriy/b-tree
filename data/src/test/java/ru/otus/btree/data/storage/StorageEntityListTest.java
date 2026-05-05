package ru.otus.btree.data.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageEntityListTest {

    @TempDir
    Path tempDir;

    private Path tempFile;
    private FileChannel fileChannel;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = tempDir.resolve("storageentity_test.dat");
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @Test
    public void testConstructorCreatesHeaderForEmptyFile() throws IOException {
        StorageEntityList list = new StorageEntityList(fileChannel);

        assertNotNull(list.getHeader());
        assertEquals(0, list.getHeader().getSize());
        assertEquals(StorageEntityList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testGetPageSize() {
        assertEquals(4096, StorageEntityList.getPageSize());
    }

    @Test
    public void testSaveAndLoadEntity() throws IOException {
        StorageEntityList list = new StorageEntityList(fileChannel);

        StorageEntity entity = createEntity(0, true, "entity0");
        list.setEntity(entity);

        StorageEntity loaded = list.getEntity(0);

        assertNotNull(loaded);
        assertEquals(entity.getId(), loaded.getId());
        assertEquals(entity.isUsed(), loaded.isUsed());
        assertEquals(entity.getName(), loaded.getName());

        assertEquals(2 * StorageEntityList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testSaveAndLoadMultipleEntities() throws IOException {
        StorageEntityList list = new StorageEntityList(fileChannel);

        StorageEntity[] entities = {
            createEntity(0, true, "entity0"),
            createEntity(1, false, "entity1"),
            createEntity(2, true, "entity2")
        };

        for (StorageEntity entity : entities) {
            list.setEntity(entity);
        }

        for (int i = 0; i < entities.length; i++) {
            StorageEntity loaded = list.getEntity(i);
            assertNotNull(loaded);
            assertEquals(entities[i].getId(), loaded.getId());
            assertEquals(entities[i].isUsed(), loaded.isUsed());
            assertEquals(entities[i].getName(), loaded.getName());
        }

        assertEquals(2 * StorageEntityList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testGetEntityNotFound() {
        StorageEntityList list = new StorageEntityList(fileChannel);

        StorageEntity loaded = list.getEntity(100);

        assertNull(loaded);
    }

    @Test
    public void testSetEntityWithNullThrowsException() {
        StorageEntityList list = new StorageEntityList(fileChannel);

        assertThrows(NullPointerException.class, () -> list.setEntity(null));
    }

    @Test
    public void testEntityAcrossPageBoundary() throws IOException {
        StorageEntityList list = new StorageEntityList(fileChannel);

        // RECORD_SIZE = 133, PAGE_SIZE = 4096
        // Record 30 starts at offset = 4096 + 30 * 133 = 8086
        // positionInPage = 8086 % 4096 = 3990
        // 3990 + 133 = 4123 > 4096, so record 30 spans two pages

        StorageEntity entity = createEntity(30, true, "boundary");
        list.setEntity(entity);

        StorageEntity loaded = list.getEntity(30);

        assertNotNull(loaded);
        assertEquals(entity.getId(), loaded.getId());
        assertEquals(entity.isUsed(), loaded.isUsed());
        assertEquals(entity.getName(), loaded.getName());

        assertEquals(3 * StorageEntityList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testFileGrowthOnSave() throws IOException {
        StorageEntityList list = new StorageEntityList(fileChannel);

        StorageEntity entity = createEntity(1000, true, "large");
        list.setEntity(entity);

        assertEquals(1001, list.getSize());

        long expectedSize = StorageEntityList.getPageSize() + 33L * StorageEntityList.getPageSize();
        assertEquals(expectedSize, fileChannel.size());
    }

    @Test
    public void testReloadFromFile() throws IOException {
        {
            StorageEntityList list = new StorageEntityList(fileChannel);
            StorageEntity entity = createEntity(5, true, "persisted");
            list.setEntity(entity);

            assertEquals(2 * StorageEntityList.getPageSize(), fileChannel.size());
        }

        fileChannel.close();
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        assertEquals(2 * StorageEntityList.getPageSize(), fileChannel.size());

        StorageEntityList list = new StorageEntityList(fileChannel);
        StorageEntity loaded = list.getEntity(5);

        assertNotNull(loaded);
        assertEquals(5, loaded.getId());
        assertTrue(loaded.isUsed());
        assertEquals("persisted", loaded.getName());

        assertEquals(6, list.getSize());
    }

    @Test
    public void testSetHeader() {
        StorageEntityList list = new StorageEntityList(fileChannel);

        StorageEntityHeader newHeader = new StorageEntityHeader();
        newHeader.setSize(42);

        list.setHeader(newHeader);

        assertEquals(newHeader, list.getHeader());
    }

    @Test
    public void testSetHeaderWithNullThrowsException() {
        StorageEntityList list = new StorageEntityList(fileChannel);

        assertThrows(NullPointerException.class, () -> list.setHeader(null));
    }

    private StorageEntity createEntity(int id, boolean isUsed, String name) {
        StorageEntity entity = new StorageEntity();
        entity.setId(id);
        entity.setUsed(isUsed);
        entity.setName(name);
        return entity;
    }
}
