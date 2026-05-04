package ru.otus.btree.lib.v1.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class StorageMangerHeader {
    private long size;

    public StorageMangerHeader() {
    }

    public StorageMangerHeader(long size) {
        this.size = size;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    /**
     * Serializes a StorageMangerHeader to a byte array.
     *
     * @param header the header to serialize
     * @return byte array containing serialized data
     */
    public static byte[] serialize(StorageMangerHeader header) {
        if (header == null) {
            return new byte[0];
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeLong(header.size);

            dos.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to serialize StorageMangerHeader", e);
        }
    }

    /**
     * Deserializes a byte array to a StorageMangerHeader.
     *
     * @param data the byte array to deserialize
     * @return the deserialized StorageMangerHeader, or null if data is null or empty
     */
    public static StorageMangerHeader deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            StorageMangerHeader header = new StorageMangerHeader();
            header.size = dis.readLong();

            return header;
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to deserialize StorageMangerHeader", e);
        }
    }
}
