package ru.otus.btree.lib.v1.storage;

import ru.otus.btree.lib.api.btree.IEntity;
import ru.otus.btree.lib.v1.btree.FileBTreeUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class RawStorage {
    private final FileChannel fileChannel;

    public RawStorage(FileChannel fileChannel) {
        this.fileChannel = Objects.requireNonNull(fileChannel, "file channel is null");
    }

    public IEntity get(long position, int size) {
        if (size <= 0) {
            return null;
        }
        try {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            fileChannel.position(position);
            int bytesRead = 0;
            while (bytesRead < size) {
                int read = fileChannel.read(buffer);
                if (read == -1) {
                    break;
                }
                bytesRead += read;
            }
            buffer.flip();
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            return FileBTreeUtils.deserializeEntity(data);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read entity from file", e);
        }
    }

    public int set(long position, IEntity entity) {
        Objects.requireNonNull(entity, "entity is null");
        try {
            byte[] data = FileBTreeUtils.serializeEntity(entity);
            ByteBuffer buffer = ByteBuffer.allocate(data.length);
            buffer.put(data);
            buffer.flip();
            fileChannel.position(position);
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
            return data.length;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write entity to file", e);
        }
    }
}
