package ru.otus.btree.lib.v1.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageMangerListTest {

    @TempDir
    Path tempDir;

    private Path tempFile;
    private FileChannel fileChannel;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = tempDir.resolve("storagemanager_test.dat");
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @Test
    public void testConstructorCreatesHeaderForEmptyFile() throws IOException {
        StorageMangerList list = new StorageMangerList(fileChannel);

        assertNotNull(list.getHeader());
        assertEquals(0, list.getHeader().getSize());
        assertEquals(StorageMangerList.PAGE_SIZE, fileChannel.size());
    }

    @Test
    public void testSaveAndLoadEntity() throws IOException {
        StorageMangerList list = new StorageMangerList(fileChannel);

        StorageManagerEntity entity = createEntity(0L, 4096, true);
        list.setEntity(entity);

        StorageManagerEntity loaded = list.getEntity(0);

        assertNotNull(loaded);
        assertEquals(entity.getPosition(), loaded.getPosition());
        assertEquals(entity.getSize(), loaded.getSize());
        assertEquals(entity.isUsed(), loaded.isUsed());

        assertEquals(2 * StorageMangerList.PAGE_SIZE, fileChannel.size());
    }

    @Test
    public void testSaveAndLoadMultipleEntities() throws IOException {
        StorageMangerList list = new StorageMangerList(fileChannel);

        StorageManagerEntity[] entities = {
            createEntity(0L, 4096, true),
            createEntity(1L, 4096, false),
            createEntity(2L, 8192, true)
        };

        for (StorageManagerEntity entity : entities) {
            list.setEntity(entity);
        }

        for (int i = 0; i < entities.length; i++) {
            StorageManagerEntity loaded = list.getEntity(i);
            assertNotNull(loaded);
            assertEquals(entities[i].getPosition(), loaded.getPosition());
            assertEquals(entities[i].getSize(), loaded.getSize());
            assertEquals(entities[i].isUsed(), loaded.isUsed());
        }

        assertEquals(2 * StorageMangerList.PAGE_SIZE, fileChannel.size());
    }

    @Test
    public void testGetEntityNotFound() {
        StorageMangerList list = new StorageMangerList(fileChannel);

        StorageManagerEntity loaded = list.getEntity(100);

        assertNull(loaded);
    }

    @Test
    public void testSetEntityWithNullThrowsException() {
        StorageMangerList list = new StorageMangerList(fileChannel);

        assertThrows(NullPointerException.class, () -> list.setEntity(null));
    }

    @Test
    public void testEntityAcrossPageBoundary() throws IOException {
        StorageMangerList list = new StorageMangerList(fileChannel);

        // PAGE_SIZE = 4096, RECORD_SIZE = 17
        // Records start at offset PAGE_SIZE
        // Record 240 starts at offset = 4096 + 240 * 17 = 4096 + 4080 = 8176
        // positionInPage = 8176 % 4096 = 4080
        // So record 240 spans pages (position 4080 + 17 = 4097 > 4096)

        StorageManagerEntity entity = createEntity(240L, 4096, true);
        list.setEntity(entity);

        StorageManagerEntity loaded = list.getEntity(240);

        assertNotNull(loaded);
        assertEquals(entity.getPosition(), loaded.getPosition());
        assertEquals(entity.getSize(), loaded.getSize());
        assertEquals(entity.isUsed(), loaded.isUsed());

        assertEquals(3 * StorageMangerList.PAGE_SIZE, fileChannel.size());
    }

    @Test
    public void testFileGrowthOnSave() throws IOException {
        StorageMangerList list = new StorageMangerList(fileChannel);

        StorageManagerEntity entity = createEntity(1000L, 4096, true);
        list.setEntity(entity);

        assertEquals(1001, list.getSize());

        long expectedSize = StorageMangerList.PAGE_SIZE + 5 * StorageMangerList.PAGE_SIZE;
        assertEquals(expectedSize, fileChannel.size());
    }

    @Test
    public void testReloadFromFile() throws IOException {
        {
            StorageMangerList list = new StorageMangerList(fileChannel);
            StorageManagerEntity entity = createEntity(5L, 4096, true);
            list.setEntity(entity);

            assertEquals(2 * StorageMangerList.PAGE_SIZE, fileChannel.size());
        }

        fileChannel.close();
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        assertEquals(2 * StorageMangerList.PAGE_SIZE, fileChannel.size());

        StorageMangerList list = new StorageMangerList(fileChannel);
        StorageManagerEntity loaded = list.getEntity(5);

        assertNotNull(loaded);
        assertEquals(5L, loaded.getPosition());
        assertEquals(4096, loaded.getSize());
        assertTrue(loaded.isUsed());

        assertEquals(6, list.getSize());
    }

    @Test
    public void testSetHeader() {
        StorageMangerList list = new StorageMangerList(fileChannel);

        StorageMangerHeader newHeader = new StorageMangerHeader();
        newHeader.setSize(42);

        list.setHeader(newHeader);

        assertEquals(newHeader, list.getHeader());
    }

    @Test
    public void testSetHeaderWithNullThrowsException() {
        StorageMangerList list = new StorageMangerList(fileChannel);

        assertThrows(NullPointerException.class, () -> list.setHeader(null));
    }

    private StorageManagerEntity createEntity(long id, int size, boolean used) {
        StorageManagerEntity entity = new StorageManagerEntity();
        entity.setPosition(id);
        entity.setSize(size);
        entity.setUsed(used);
        return entity;
    }
}
