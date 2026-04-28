package ru.otus.btree.lib.v1.btree;

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
public class PageManagerListTest {

    @TempDir
    Path tempDir;

    private Path tempFile;
    private FileChannel fileChannel;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = tempDir.resolve("pagemanager_test.dat");
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    }

    @Test
    public void testConstructorCreatesHeaderForEmptyFile() throws IOException {
        PageManagerList list = new PageManagerList(fileChannel);

        assertNotNull(list.getHeader());
        assertEquals(0, list.getHeader().getSize());
        assertEquals(PageManagerList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testGetPageSize() {
        assertEquals(4096, PageManagerList.getPageSize());
    }

    @Test
    public void testSaveAndLoadEntity() throws IOException {
        PageManagerList list = new PageManagerList(fileChannel);

        PageManagerEntity entity = createEntity(0L, 4096, true);
        list.setEntity(entity);

        PageManagerEntity loaded = list.getEntity(0);

        assertNotNull(loaded);
        assertEquals(entity.getId(), loaded.getId());
        assertEquals(entity.getSize(), loaded.getSize());
        assertEquals(entity.isUsed(), loaded.isUsed());

        // Verify file has grown to accommodate the entity
        // Header page (4096) + at least one data page (4096) = 8192
        assertEquals(2 * PageManagerList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testSaveAndLoadMultipleEntities() throws IOException {
        PageManagerList list = new PageManagerList(fileChannel);

        PageManagerEntity[] entities = {
            createEntity(0L, 4096, true),
            createEntity(1L, 4096, false),
            createEntity(2L, 8192, true)
        };

        for (PageManagerEntity entity : entities) {
            list.setEntity(entity);
        }

        for (int i = 0; i < entities.length; i++) {
            PageManagerEntity loaded = list.getEntity(i);
            assertNotNull(loaded);
            assertEquals(entities[i].getId(), loaded.getId());
            assertEquals(entities[i].getSize(), loaded.getSize());
            assertEquals(entities[i].isUsed(), loaded.isUsed());
        }

        // Verify file size: header page (4096) + one data page (4096) = 8192
        // All 3 entities fit within one data page
        assertEquals(2 * PageManagerList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testGetEntityNotFound() {
        PageManagerList list = new PageManagerList(fileChannel);

        PageManagerEntity loaded = list.getEntity(100);

        assertNull(loaded);
    }

    @Test
    public void testSetEntityWithNullThrowsException() {
        PageManagerList list = new PageManagerList(fileChannel);

        assertThrows(NullPointerException.class, () -> list.setEntity(null));
    }

    @Test
    public void testEntityAcrossPageBoundary() throws IOException {
        PageManagerList list = new PageManagerList(fileChannel);

        // Calculate index where record spans two pages
        // PAGE_SIZE = 4096, RECORD_SIZE = 13
        // Records start at offset PAGE_SIZE
        // We need positionInPage + RECORD_SIZE > PAGE_SIZE
        // PAGE_SIZE / RECORD_SIZE = 315 records per page
        // Record 315 starts at offset = 4096 + 315 * 13 = 4096 + 4095 = 8191
        // positionInPage = 8191 % 4096 = 4095
        // So record 315 spans pages (position 4095 + 13 = 4108 > 4096)

        PageManagerEntity entity = createEntity(315L, 4096, true);
        list.setEntity(entity);

        PageManagerEntity loaded = list.getEntity(315);

        assertNotNull(loaded);
        assertEquals(entity.getId(), loaded.getId());
        assertEquals(entity.getSize(), loaded.getSize());
        assertEquals(entity.isUsed(), loaded.isUsed());

        // Record 315 spans two data pages, so file should have 3 pages total:
        // header page (4096) + data page 1 (4096) + data page 2 (4096) = 12288
        assertEquals(3 * PageManagerList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testFileGrowthOnSave() throws IOException {
        PageManagerList list = new PageManagerList(fileChannel);

        // Save entity at high index
        PageManagerEntity entity = createEntity(1000L, 4096, true);
        list.setEntity(entity);

        // Verify header size was updated
        assertEquals(1001, list.getSize());

        // File should have grown to accommodate the record
        // Header page (4096) + data pages needed for record 1000
        // 1000 records * 13 bytes = 13000 bytes, need 4 data pages (4 * 4096 = 16384)
        // Total: 4096 + 16384 = 20480 bytes
        long expectedSize = PageManagerList.getPageSize() + 4 * PageManagerList.getPageSize();
        assertEquals(expectedSize, fileChannel.size());
    }

    @Test
    public void testReloadFromFile() throws IOException {
        // First session: create and save entities
        {
            PageManagerList list = new PageManagerList(fileChannel);
            PageManagerEntity entity = createEntity(5L, 4096, true);
            list.setEntity(entity);

            // Verify file size after save: header page (4096) + data page (4096) = 8192
            assertEquals(2 * PageManagerList.getPageSize(), fileChannel.size());
        }

        // Reopen file channel to simulate new session
        fileChannel.close();
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // Verify file size after reopen
        assertEquals(2 * PageManagerList.getPageSize(), fileChannel.size());

        // Second session: reload and verify
        PageManagerList list = new PageManagerList(fileChannel);
        PageManagerEntity loaded = list.getEntity(5);

        assertNotNull(loaded);
        assertEquals(5L, loaded.getId());
        assertEquals(4096, loaded.getSize());
        assertTrue(loaded.isUsed());

        // Verify header size was persisted correctly
        assertEquals(6, list.getSize());
    }

    @Test
    public void testSetHeader() {
        PageManagerList list = new PageManagerList(fileChannel);

        PageManagerHeader newHeader = new PageManagerHeader();
        newHeader.setSize(42);

        list.setHeader(newHeader);

        assertEquals(newHeader, list.getHeader());
    }

    @Test
    public void testSetHeaderWithNullThrowsException() {
        PageManagerList list = new PageManagerList(fileChannel);

        assertThrows(NullPointerException.class, () -> list.setHeader(null));
    }

    private PageManagerEntity createEntity(long id, int size, boolean used) {
        PageManagerEntity entity = new PageManagerEntity();
        entity.setId(id);
        entity.setSize(size);
        entity.setUsed(used);
        return entity;
    }
}
