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
public class StorageIndexListTest {

    @TempDir
    Path tempDir;

    private Path tempFile;
    private FileChannel fileChannel;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = tempDir.resolve("storageindex_test.dat");
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @Test
    public void testConstructorCreatesHeaderForEmptyFile() throws IOException {
        StorageIndexList list = new StorageIndexList(fileChannel);

        assertNotNull(list.getHeader());
        assertEquals(0, list.getHeader().getSize());
        assertEquals(StorageIndexList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testGetPageSize() {
        assertEquals(4096, StorageIndexList.getPageSize());
    }

    @Test
    public void testSaveAndLoadIndex() throws IOException {
        StorageIndexList list = new StorageIndexList(fileChannel);

        StorageIndex index = createIndex(0, true, "entity0", "field0");
        list.setIndex(index);

        StorageIndex loaded = list.getIndex(0);

        assertNotNull(loaded);
        assertEquals(index.getId(), loaded.getId());
        assertEquals(index.isUsed(), loaded.isUsed());
        assertEquals(index.getEntityName(), loaded.getEntityName());
        assertEquals(index.getFieldName(), loaded.getFieldName());

        assertEquals(2 * StorageIndexList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testSaveAndLoadMultipleIndexes() throws IOException {
        StorageIndexList list = new StorageIndexList(fileChannel);

        StorageIndex[] indexes = {
            createIndex(0, true, "entity0", "field0"),
            createIndex(1, false, "entity1", "field1"),
            createIndex(2, true, "entity2", "field2")
        };

        for (StorageIndex index : indexes) {
            list.setIndex(index);
        }

        for (int i = 0; i < indexes.length; i++) {
            StorageIndex loaded = list.getIndex(i);
            assertNotNull(loaded);
            assertEquals(indexes[i].getId(), loaded.getId());
            assertEquals(indexes[i].isUsed(), loaded.isUsed());
            assertEquals(indexes[i].getEntityName(), loaded.getEntityName());
            assertEquals(indexes[i].getFieldName(), loaded.getFieldName());
        }

        assertEquals(2 * StorageIndexList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testGetIndexNotFound() {
        StorageIndexList list = new StorageIndexList(fileChannel);

        StorageIndex loaded = list.getIndex(100);

        assertNull(loaded);
    }

    @Test
    public void testSetIndexWithNullThrowsException() {
        StorageIndexList list = new StorageIndexList(fileChannel);

        assertThrows(NullPointerException.class, () -> list.setIndex(null));
    }

    @Test
    public void testIndexAcrossPageBoundary() throws IOException {
        StorageIndexList list = new StorageIndexList(fileChannel);

        // RECORD_SIZE = 261, PAGE_SIZE = 4096
        // Record 31 starts at offset = 4096 + 31 * 261 = 12187
        // positionInPage = 12187 % 4096 = 3995
        // 3995 + 261 = 4256 > 4096, so record 31 spans two pages

        StorageIndex index = createIndex(31, true, "boundary-entity", "boundary-field");
        list.setIndex(index);

        StorageIndex loaded = list.getIndex(31);

        assertNotNull(loaded);
        assertEquals(index.getId(), loaded.getId());
        assertEquals(index.isUsed(), loaded.isUsed());
        assertEquals(index.getEntityName(), loaded.getEntityName());
        assertEquals(index.getFieldName(), loaded.getFieldName());

        assertEquals(4 * StorageIndexList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testFileGrowthOnSave() throws IOException {
        StorageIndexList list = new StorageIndexList(fileChannel);

        StorageIndex index = createIndex(1000, true, "large-entity", "large-field");
        list.setIndex(index);

        assertEquals(1001, list.getSize());

        long expectedSize = 65L * StorageIndexList.getPageSize();
        assertEquals(expectedSize, fileChannel.size());
    }

    @Test
    public void testReloadFromFile() throws IOException {
        {
            StorageIndexList list = new StorageIndexList(fileChannel);
            StorageIndex index = createIndex(5, true, "persisted-entity", "persisted-field");
            list.setIndex(index);

            assertEquals(2 * StorageIndexList.getPageSize(), fileChannel.size());
        }

        fileChannel.close();
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        assertEquals(2 * StorageIndexList.getPageSize(), fileChannel.size());

        StorageIndexList list = new StorageIndexList(fileChannel);
        StorageIndex loaded = list.getIndex(5);

        assertNotNull(loaded);
        assertEquals(5, loaded.getId());
        assertTrue(loaded.isUsed());
        assertEquals("persisted-entity", loaded.getEntityName());
        assertEquals("persisted-field", loaded.getFieldName());

        assertEquals(6, list.getSize());
    }

    @Test
    public void testSetHeader() {
        StorageIndexList list = new StorageIndexList(fileChannel);

        StorageIndexHeader newHeader = new StorageIndexHeader();
        newHeader.setSize(42);

        list.setHeader(newHeader);

        assertEquals(newHeader, list.getHeader());
    }

    @Test
    public void testSetHeaderWithNullThrowsException() {
        StorageIndexList list = new StorageIndexList(fileChannel);

        assertThrows(NullPointerException.class, () -> list.setHeader(null));
    }

    private StorageIndex createIndex(int id, boolean isUsed, String entityName, String fieldName) {
        StorageIndex index = new StorageIndex();
        index.setId(id);
        index.setUsed(isUsed);
        index.setEntityName(entityName);
        index.setFieldName(fieldName);
        return index;
    }
}
