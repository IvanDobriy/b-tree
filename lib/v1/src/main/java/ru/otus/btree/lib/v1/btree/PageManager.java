package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.v1.array.SingleArray;

import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Manages page allocation for the B-tree file.
 * Each page has a fixed size (PAGE_SIZE = 4096 bytes).
 */
public class PageManager {
    private static final int PAGE_SIZE = 4096;
    private final FileChannel fileChannel;
    private final PageManagerList pageManagerList;
    private final IArray<PageManagerEntity> deletedEntities;

    public PageManager(FileChannel fileChannel) {
        Objects.requireNonNull(fileChannel, "file channel is null");
        this.fileChannel = fileChannel;
        this.pageManagerList = new PageManagerList(fileChannel);
        this.deletedEntities = collectDeletedEntities();
    }

    /**
     * Allocates a new page and returns its page ID.
     * First checks for deleted entities to reuse, otherwise creates a new one.
     * Page ID is the byte offset in the file.
     *
     * @return the newly allocated page ID
     */
    public long allocatePage() {
        long pageId;

        // Check if there are deleted entities to reuse
        if (deletedEntities.size() > 0) {
            // Get the last deleted entity and remove it from the list
            int lastIndex = deletedEntities.size() - 1;
            PageManagerEntity reusedEntity = deletedEntities.get(lastIndex);
            deletedEntities.remove(lastIndex);

            // Mark as used and save
            reusedEntity.setUsed(true);
            pageManagerList.setEntity(reusedEntity);

            pageId = reusedEntity.getId() * PAGE_SIZE;
        } else {
            // Allocate new page based on header size
            long newPageIndex = pageManagerList.getHeader().getSize();
            pageId = newPageIndex * PAGE_SIZE;

            // Create new entity
            PageManagerEntity newEntity = new PageManagerEntity();
            newEntity.setId(newPageIndex);
            newEntity.setSize(PAGE_SIZE);
            newEntity.setUsed(true);

            // Add to pageManagerList (this will update header size)
            pageManagerList.setEntity(newEntity);
        }

        return pageId;
    }

    /**
     * Returns the page size.
     *
     * @return page size in bytes
     */
    public int getPageSize() {
        return PAGE_SIZE;
    }

    /**
     * Collects all unused (deleted) page entities from the PageManagerList.
     * An entity is considered deleted if isUsed() returns false.
     *
     * @return IArray of unused PageManagerEntity objects
     */
    private IArray<PageManagerEntity> collectDeletedEntities() {
        IArray<PageManagerEntity> deletedEntities = new SingleArray<>(0);
        PageManagerHeader header = pageManagerList.getHeader();
        int totalEntities = (int) header.getSize();

        for (int i = 0; i < totalEntities; i++) {
            PageManagerEntity entity = pageManagerList.getEntity(i);
            if (entity != null && !entity.isUsed()) {
                deletedEntities.add(deletedEntities.size(), entity);
            }
        }

        return deletedEntities;
    }
}
