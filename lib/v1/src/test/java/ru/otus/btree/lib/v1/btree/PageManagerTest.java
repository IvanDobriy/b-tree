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

        // First page ID is 0 (page index 0 from header size)
        assertEquals(0, pageId);

        // Verify file size: header page (4096) + data page (4096) = 8192
        assertEquals(2 * PageManagerList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testAllocateMultiplePages() throws IOException {
        PageManager pageManager = new PageManager(fileChannel);

        long pageId1 = pageManager.allocatePage();
        long pageId2 = pageManager.allocatePage();
        long pageId3 = pageManager.allocatePage();

        assertEquals(0, pageId1);                                  // 0
        assertEquals(PageManagerList.getPageSize(), pageId2);     // 4096
        assertEquals(2L * PageManagerList.getPageSize(), pageId3); // 8192

        // Verify file size: header page + 2 data pages = 3 * 4096 = 12288
        assertEquals(3 * PageManagerList.getPageSize(), fileChannel.size());
    }

    @Test
    public void testAllocatePageWithDifferentSizes() throws IOException {
        PageManager pageManager = new PageManager(fileChannel);

        long pageId1 = pageManager.allocatePage();
        long pageId2 = pageManager.allocatePage();
        long pageId3 = pageManager.allocatePage();

        assertEquals(0, pageId1);
        assertEquals(PageManagerList.getPageSize(), pageId2);
        assertEquals(2L * PageManagerList.getPageSize(), pageId3);
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

        assertEquals(0, pageId1);
        assertEquals(PageManagerList.getPageSize(), pageId2);

        // Close file channel
        fileChannel.close();

        // Reopen file channel to simulate new session
        fileChannel = FileChannel.open(tempFile, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // Second session: create new PageManager and verify existing pages
        PageManager pageManager2 = new PageManager(fileChannel);

        // Allocate new page - should continue from where we left off
        long pageId3 = pageManager2.allocatePage();
        assertEquals(2L * PageManagerList.getPageSize(), pageId3);
    }
}
