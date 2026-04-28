package ru.otus.btree.lib.v1.btree;

import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages page allocation for the B-tree file.
 * Each page has a fixed size (PAGE_SIZE = 4096 bytes).
 */
public class PageManager {
    private static final int PAGE_SIZE = 4096;
    private final FileChannel fileChannel;
    private final PageManagerList pageManagerList;
    private final AtomicLong nextPageId;

    public PageManager(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        this.pageManagerList = new PageManagerList(fileChannel);
        // Start after header page (page 0 is reserved for header/root info)
        this.nextPageId = new AtomicLong(PAGE_SIZE);
    }

    /**
     * Allocates a new page and returns its page ID.
     * Page ID is the byte offset in the file.
     *
     * @return the newly allocated page ID
     */
    public long allocatePage() {
        return nextPageId.getAndAdd(PAGE_SIZE);
    }

    /**
     * Allocates a new page for the root node (always at page 0).
     *
     * @return 0 (root page ID)
     */
    public long allocateRootPage() {
        return 0;
    }

    /**
     * Gets the next page ID that would be allocated without actually allocating it.
     *
     * @return the next available page ID
     */
    public long getNextPageId() {
        return nextPageId.get();
    }

    /**
     * Returns the page size.
     *
     * @return page size in bytes
     */
    public int getPageSize() {
        return PAGE_SIZE;
    }
}
