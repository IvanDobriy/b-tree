package ru.otus.btree.lib.v1.btree;

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
public class PageManagerTest {

    @TempDir
    Path tempDir;

    private Path tempFile;
    private FileChannel fileChannel;

    @BeforeEach
    public void setUp() throws IOException {
        tempFile = tempDir.resolve("pagemanager_test.dat");
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE);
    }

    @Test
    public void testConstructorCreatesPageManagerList() throws IOException {
        PageManager pageManager = new PageManager(fileChannel);

        assertNotNull(pageManager);
        assertEquals(PageManagerList.getPageSize(), pageManager.getPageSize());
    }

    @Test
    public void testAllocatePageForEmptyFile() throws IOException {
        PageManager pageManager = new PageManager(fileChannel);

        long pageId = pageManager.allocatePage();

        // First page ID should be 0 (after header at page 0)
        // Actually page 0 is header, so first data page is at PAGE_SIZE (4096)
        assertEquals(PageManagerList.getPageSize(), pageId);

        // Verify file size: header page + one data page = 8192
        assertEquals(2 * PageManagerList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testAllocateMultiplePages() throws IOException {
        PageManager pageManager = new PageManager(fileChannel);

        long pageId1 = pageManager.allocatePage();
        long pageId2 = pageManager.allocatePage();
        long pageId3 = pageManager.allocatePage();

        assertEquals(PageManagerList.getPageSize(), pageId1);     // 4096
        assertEquals(2L * PageManagerList.getPageSize(), pageId2); // 8192
        assertEquals(3L * PageManagerList.getPageSize(), pageId3); // 12288

        // Verify file size: header page + 3 data pages = 4 * 4096 = 16384
        assertEquals(4 * PageManagerList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testAllocatePageWithDifferentSizes() throws IOException {
        PageManager pageManager = new PageManager(fileChannel);

        long pageId1 = pageManager.allocatePage();
        long pageId2 = pageManager.allocatePage();
        long pageId3 = pageManager.allocatePage();

        assertEquals(PageManagerList.getPageSize(), pageId1);
        assertEquals(2L * PageManagerList.getPageSize(), pageId2);
        assertEquals(3L * PageManagerList.getPageSize(), pageId3);
    }

    @Test
    public void testGetPageSize() {
        PageManager pageManager = new PageManager(fileChannel);

        assertEquals(4096, pageManager.getPageSize());
    }

    @Test
    public void testConstructorWithNullFileChannel() {
        assertThrows(NullPointerException.class, () -> {
            new PageManager(null);
        });
    }

    @Test
    public void testReloadFromFile() throws IOException {
        // First session: allocate some pages
        PageManager pageManager1 = new PageManager(fileChannel);
        long pageId1 = pageManager1.allocatePage();
        long pageId2 = pageManager1.allocatePage();

        assertEquals(PageManagerList.getPageSize(), pageId1);
        assertEquals(2L * PageManagerList.getPageSize(), pageId2);

        // Close file channel
        fileChannel.close();

        // Reopen file channel to simulate new session
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // Second session: create new PageManager and verify existing pages
        PageManager pageManager2 = new PageManager(fileChannel);

        // Allocate new page - should continue from where we left off
        long pageId3 = pageManager2.allocatePage();
        assertEquals(3L * PageManagerList.getPageSize(), pageId3);
    }
}
