package ru.otus.btree.lib.v1.storage;

import ru.otus.btree.lib.v1.storage.StorageMangerHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class StorageMangerList {
    public static final int PAGE_SIZE = 4096;
    private final FileChannel fileChannel;
    private StorageMangerHeader header;

    public StorageMangerList(FileChannel fileChannel) {
        this.fileChannel = Objects.requireNonNull(fileChannel, "fileChannel must not be null");
        this.header = loadHeader();
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public StorageMangerHeader getHeader() {
        return header;
    }

    public void setHeader(StorageMangerHeader header) {
        this.header = Objects.requireNonNull(header, "header must not be null");
    }

    /**
     * Loads header from the file channel.
     * Reads and deserializes the first page (PAGE_SIZE).
     * If file is empty, creates a new header with size = 0, saves it and returns.
     *
     * @return the loaded or newly created StorageMangerHeader
     */
    private StorageMangerHeader loadHeader() {
        try {
            // Check if file is empty
            if (fileChannel.size() == 0) {
                // Create new header with size = 0
                StorageMangerHeader newHeader = new StorageMangerHeader();
                newHeader.setSize(0);

                // Serialize and save to file
                byte[] headerData = StorageMangerHeader.serialize(newHeader);
                ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
                buffer.put(headerData);
                buffer.clear();

                fileChannel.position(0);
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }

                return newHeader;
            }

            // Read header from first page
            ByteBuffer buffer = readPage(0);
            int bytesRead = buffer.remaining();

            if (bytesRead <= 0) {
                // Empty read, create new header
                StorageMangerHeader newHeader = new StorageMangerHeader();
                newHeader.setSize(0);
                return newHeader;
            }

            byte[] headerData = new byte[bytesRead];
            buffer.get(headerData);

            // Deserialize header
            StorageMangerHeader loadedHeader = StorageMangerHeader.deserialize(headerData);
            if (loadedHeader == null) {
                // Failed to deserialize, create new header
                loadedHeader = new StorageMangerHeader();
                loadedHeader.setSize(0);
            }

            return loadedHeader;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load header", e);
        }
    }

    /**
     * Loads a single page record from the file channel by index.
     *
     * @param recordIndex the index of the record to load
     * @return the loaded StorageManagerEntity, or null if record doesn't exist
     */
    private StorageManagerEntity loadPageRecord(int recordIndex) {
        try {
            long offset = calculateOffset(recordIndex);
            long fileSize = fileChannel.size();

            if (offset + StorageManagerEntity.RECORD_SIZE > fileSize) {
                return null;
            }

            int pageIndex = (int) (offset / PAGE_SIZE);
            int positionInPage = (int) (offset % PAGE_SIZE);
            byte[] recordData = new byte[StorageManagerEntity.RECORD_SIZE];

            if (positionInPage + StorageManagerEntity.RECORD_SIZE <= PAGE_SIZE) {
                // Record fits entirely within one page
                ByteBuffer buffer = readPage(pageIndex);
                if (buffer.remaining() < positionInPage + StorageManagerEntity.RECORD_SIZE) {
                    throw new IOException("Incomplete data read from page " + pageIndex);
                }
                buffer.position(positionInPage);
                buffer.get(recordData, 0, StorageManagerEntity.RECORD_SIZE);
            } else {
                // Record spans across two pages
                int bytesFromFirstPage = PAGE_SIZE - positionInPage;
                int bytesFromSecondPage = StorageManagerEntity.RECORD_SIZE - bytesFromFirstPage;

                // Read first page
                ByteBuffer buffer1 = readPage(pageIndex);
                if (buffer1.remaining() < bytesFromFirstPage) {
                    throw new IOException("Incomplete data in first page " + pageIndex);
                }
                buffer1.position(positionInPage);
                buffer1.get(recordData, 0, bytesFromFirstPage);

                // Read second page
                ByteBuffer buffer2 = readPage(pageIndex + 1);
                if (buffer2.remaining() < bytesFromSecondPage) {
                    throw new IOException("Incomplete data in second page " + (pageIndex + 1));
                }
                buffer2.get(recordData, bytesFromFirstPage, bytesFromSecondPage);
            }

            return StorageManagerEntity.deserialize(recordData);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load page record", e);
        }
    }

    /**
     * Calculates the offset for a record in the file.
     *
     * @param recordIndex the index of the record
     * @return the calculated offset
     */
    private long calculateOffset(int recordIndex) {
        return PAGE_SIZE + (long) recordIndex * StorageManagerEntity.RECORD_SIZE;
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
}
