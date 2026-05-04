package ru.otus.btree.lib.v1.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class StorageManagerEntity {
    public static final int RECORD_SIZE = 17;

    private long position;
    private int size;
    private boolean used;
    private int id;

    public StorageManagerEntity() {
    }

    public StorageManagerEntity(long position, int size, boolean used, int id) {
        this.position = position;
        this.size = size;
        this.used = used;
        this.id = id;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean isUsed() {
        return used;
    }

    public void setUsed(boolean used) {
        this.used = used;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    /**
     * Serializes a StorageManagerEntity to a byte array.
     *
     * @param entity the entity to serialize
     * @return byte array containing serialized data
     */
    public static byte[] serialize(StorageManagerEntity entity) {
        if (entity == null) {
            return new byte[0];
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeLong(entity.position);
            dos.writeInt(entity.size);
            dos.writeBoolean(entity.used);
            dos.writeInt(entity.id);

            dos.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to serialize StorageManagerEntity", e);
        }
    }

    /**
     * Deserializes a byte array to a StorageManagerEntity.
     *
     * @param data the byte array to deserialize
     * @return the deserialized StorageManagerEntity, or null if data is null or empty
     */
    public static StorageManagerEntity deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            StorageManagerEntity entity = new StorageManagerEntity();
            entity.position = dis.readLong();
            entity.size = dis.readInt();
            entity.used = dis.readBoolean();
            entity.id = dis.readInt();

            return entity;
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to deserialize StorageManagerEntity", e);
        }
    }
}
