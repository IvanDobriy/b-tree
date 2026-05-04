package ru.otus.btree.lib.v1.storage;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.v1.array.SingleArray;

import java.nio.channels.FileChannel;
import java.util.Objects;

public class StorageManager {
    private static final int PAGE_SIZE = 4096;
    private final StorageMangerList storageMangerList;
    private final IArray<StorageManagerEntity> deletedEntities;

    public StorageManager(FileChannel fileChannel) {
        Objects.requireNonNull(fileChannel, "file channel is null");
        this.storageMangerList = new StorageMangerList(fileChannel);
        this.deletedEntities = collectDeletedEntities();
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
     * Allocates a new position and returns its position ID.
     * First checks for deleted entities to reuse, otherwise creates a new one.
     * Position ID is the byte offset in the file.
     *
     * @param entitySize the size of the entity to allocate
     * @return the newly allocated position ID
     */
    public long allocatePosition(int entitySize) {
        long position;

        // Check if there are deleted entities to reuse
        if (deletedEntities.size() > 0) {
            // Get the last deleted entity and remove it from the list
            int lastIndex = deletedEntities.size() - 1;
            StorageManagerEntity reusedEntity = deletedEntities.get(lastIndex);
            deletedEntities.remove(lastIndex);

            // Mark as used and save
            reusedEntity.setUsed(true);
            reusedEntity.setSize(entitySize);
            storageMangerList.setEntity(reusedEntity);

            position = reusedEntity.getId();
        } else {
            // Allocate new position based on header file size
            position = storageMangerList.getHeader().getFileSize();

            // Create new entity
            StorageManagerEntity newEntity = new StorageManagerEntity();
            newEntity.setId(position);
            newEntity.setSize(entitySize);
            newEntity.setUsed(true);

            // Add to storageMangerList (this will update header size)
            storageMangerList.setEntity(newEntity);
        }

        return position;
    }

    /**
     * Collects all unused (deleted) page entities from the StorageMangerList.
     * An entity is considered deleted if isUsed() returns false.
     *
     * @return IArray of unused PageManagerEntity objects
     */
    private IArray<StorageManagerEntity> collectDeletedEntities() {
        IArray<StorageManagerEntity> deletedEntities = new SingleArray<>(0);
        StorageMangerHeader header = storageMangerList.getHeader();
        int totalEntities = (int) header.getSize();

        for (int i = 0; i < totalEntities; i++) {
            StorageManagerEntity entity = storageMangerList.getEntity(i);
            if (entity != null && !entity.isUsed()) {
                deletedEntities.add(deletedEntities.size(), entity);
            }
        }

        return deletedEntities;
    }
}
