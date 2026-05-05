package ru.otus.btree.data.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Manages a list of storage entities using fixed-size records.
 * Provides operations to add, remove, find and manage entities.
 */
public class StorageEntityList {
    public static final int PAGE_SIZE = 4096;
    private final FileChannel fileChannel;
    private StorageEntityHeader header;

    public StorageEntityList(FileChannel fileChannel) {
        this.fileChannel = Objects.requireNonNull(fileChannel, "fileChannel must not be null");
        this.header = loadHeader();
    }

    public StorageEntityHeader getHeader() {
        return header;
    }

    public void setHeader(StorageEntityHeader header) {
        this.header = Objects.requireNonNull(header, "header must not be null");
    }

    /**
     * Returns the page size.
     *
     * @return page size in bytes
     */
    public static int getPageSize() {
        return PAGE_SIZE;
    }

    /**
     * Returns the number of entities in the list.
     *
     * @return the entity count
     */
    public int getSize() {
        return header.getSize();
    }

    /**
     * Gets a storage entity by index.
     * Loads the entity from file if it exists.
     *
     * @param entityIndex the index of the entity to load
     * @return the StorageEntity, or null if not found
     */
    public StorageEntity getEntity(int entityIndex) {
        return loadRecord(entityIndex);
    }

    /**
     * Saves a storage entity to the file.
     * Uses entity.getId() as the record index and saveRecord to persist the entity.
     * If the entity's ID exceeds current header size, updates header size and saves it.
     *
     * @param entity the entity to save
     */
    public void setEntity(StorageEntity entity) {
        Objects.requireNonNull(entity, "entity must not be null");

        int entityId = entity.getId();
        if (entityId >= header.getSize()) {
            header.setSize(entityId + 1);
            saveHeader();
        }

        saveRecord(entity);
    }

    /**
     * Loads header from the file channel.
     * Reads and deserializes the first page (PAGE_SIZE).
     * If file is empty, creates a new header with size = 0, saves it and returns.
     *
     * @return the loaded or newly created StorageEntityHeader
     */
    private StorageEntityHeader loadHeader() {
        try {
            if (fileChannel.size() == 0) {
                StorageEntityHeader newHeader = new StorageEntityHeader();
                newHeader.setSize(0);

                byte[] headerData = StorageEntityHeader.serialize(newHeader);
                ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
                buffer.put(headerData);
                buffer.clear();

                fileChannel.position(0);
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }

                return newHeader;
            }

            ByteBuffer buffer = readPage(0);
            int bytesRead = buffer.remaining();

            if (bytesRead <= 0) {
                StorageEntityHeader newHeader = new StorageEntityHeader();
                newHeader.setSize(0);
                return newHeader;
            }

            byte[] headerData = new byte[bytesRead];
            buffer.get(headerData);

            StorageEntityHeader loadedHeader = StorageEntityHeader.deserialize(headerData);
            if (loadedHeader == null) {
                loadedHeader = new StorageEntityHeader();
                loadedHeader.setSize(0);
            }

            return loadedHeader;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load header", e);
        }
    }

    /**
     * Loads a single record from the file channel by index.
     * Data is read block by block. If the record spans across page boundaries,
     * the next block is loaded to complete the record.
     *
     * @param recordIndex the index of the record to load
     * @return the loaded StorageEntity, or null if record doesn't exist
     */
    private StorageEntity loadRecord(int recordIndex) {
        try {
            long offset = calculateOffset(recordIndex);
            long fileSize = fileChannel.size();

            if (offset + StorageEntity.RECORD_SIZE > fileSize) {
                return null;
            }

            int pageIndex = (int) (offset / PAGE_SIZE);
            int positionInPage = (int) (offset % PAGE_SIZE);
            byte[] recordData = new byte[StorageEntity.RECORD_SIZE];

            if (positionInPage + StorageEntity.RECORD_SIZE <= PAGE_SIZE) {
                ByteBuffer buffer = readPage(pageIndex);
                if (buffer.remaining() < positionInPage + StorageEntity.RECORD_SIZE) {
                    throw new IOException("Incomplete data read from page " + pageIndex);
                }
                buffer.position(positionInPage);
                buffer.get(recordData, 0, StorageEntity.RECORD_SIZE);
            } else {
                int bytesFromFirstPage = PAGE_SIZE - positionInPage;
                int bytesFromSecondPage = StorageEntity.RECORD_SIZE - bytesFromFirstPage;

                ByteBuffer buffer1 = readPage(pageIndex);
                if (buffer1.remaining() < bytesFromFirstPage) {
                    throw new IOException("Incomplete data in first page " + pageIndex);
                }
                buffer1.position(positionInPage);
                buffer1.get(recordData, 0, bytesFromFirstPage);

                ByteBuffer buffer2 = readPage(pageIndex + 1);
                if (buffer2.remaining() < bytesFromSecondPage) {
                    throw new IOException("Incomplete data in second page " + (pageIndex + 1));
                }
                buffer2.get(recordData, bytesFromFirstPage, bytesFromSecondPage);
            }

            return StorageEntity.deserialize(recordData);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load record", e);
        }
    }

    /**
     * Calculates the offset for a record in the file.
     *
     * @param recordIndex the index of the record
     * @return the calculated offset
     */
    private long calculateOffset(int recordIndex) {
        return PAGE_SIZE + (long) recordIndex * StorageEntity.RECORD_SIZE;
    }

    /**
     * Saves a record to the file channel.
     * Uses entity.getId() as the record index.
     * Data is written block by block. If the record spans across page boundaries,
     * both pages are written to complete the record.
     *
     * @param entity the entity to save
     */
    private void saveRecord(StorageEntity entity) {
        Objects.requireNonNull(entity, "entity must not be null");

        try {
            byte[] recordData = StorageEntity.serialize(entity);
            long offset = calculateOffset(entity.getId());

            int pageIndex = (int) (offset / PAGE_SIZE);
            int positionInPage = (int) (offset % PAGE_SIZE);

            long requiredSize = (long) (pageIndex + 1) * PAGE_SIZE;
            if (positionInPage + StorageEntity.RECORD_SIZE > PAGE_SIZE) {
                requiredSize = (long) (pageIndex + 2) * PAGE_SIZE;
            }
            ensureFileSize(requiredSize);

            if (positionInPage + StorageEntity.RECORD_SIZE <= PAGE_SIZE) {
                ByteBuffer buffer = readPage(pageIndex);
                if (buffer.remaining() < positionInPage) {
                    throw new IOException("Incomplete page data at page " + pageIndex);
                }
                buffer.position(positionInPage);
                buffer.put(recordData, 0, StorageEntity.RECORD_SIZE);

                buffer.flip();
                writePage(pageIndex, buffer);
            } else {
                int bytesInFirstPage = PAGE_SIZE - positionInPage;
                int bytesInSecondPage = StorageEntity.RECORD_SIZE - bytesInFirstPage;

                ByteBuffer buffer1 = readPage(pageIndex);
                if (buffer1.remaining() < positionInPage) {
                    throw new IOException("Incomplete page data at page " + pageIndex);
                }
                buffer1.position(positionInPage);
                buffer1.put(recordData, 0, bytesInFirstPage);

                buffer1.flip();
                writePage(pageIndex, buffer1);

                ByteBuffer buffer2 = readPage(pageIndex + 1);
                buffer2.put(recordData, bytesInFirstPage, bytesInSecondPage);

                buffer2.flip();
                writePage(pageIndex + 1, buffer2);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save record", e);
        }
    }

    /**
     * Ensures the file is at least the specified size by extending it if necessary.
     *
     * @param requiredSize the minimum required file size
     * @throws IOException if an I/O error occurs
     */
    private void ensureFileSize(long requiredSize) throws IOException {
        long currentSize = fileChannel.size();
        if (currentSize < requiredSize) {
            fileChannel.position(currentSize);
            ByteBuffer emptyPage = ByteBuffer.allocate(PAGE_SIZE);
            while (fileChannel.position() < requiredSize) {
                emptyPage.clear();
                fileChannel.write(emptyPage);
            }
        }
    }

    /**
     * Reads a single page into a ByteBuffer from the file channel.
     *
     * @param pageIndex the index of the page to read
     * @return ByteBuffer containing the page data
     * @throws IOException if an I/O error occurs
     */
    private ByteBuffer readPage(int pageIndex) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        fileChannel.position((long) pageIndex * PAGE_SIZE);
        int bytesRead = fileChannel.read(buffer);

        if (bytesRead < 0) {
            throw new IOException("Failed to read page " + pageIndex + ": end of file reached");
        }

        buffer.flip();
        return buffer;
    }

    /**
     * Writes a ByteBuffer to the specified page in the file channel.
     *
     * @param pageIndex the index of the page to write
     * @param buffer    the buffer containing data to write
     * @throws IOException if an I/O error occurs
     */
    private void writePage(int pageIndex, ByteBuffer buffer) throws IOException {
        Objects.requireNonNull(buffer, "buffer must not be null");

        fileChannel.position((long) pageIndex * PAGE_SIZE);
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
    }

    /**
     * Saves the header to the file channel.
     */
    private void saveHeader() {
        try {
            byte[] headerData = StorageEntityHeader.serialize(header);
            ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
            buffer.put(headerData);
            buffer.flip();

            fileChannel.position(0);
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save header", e);
        }
    }
}
