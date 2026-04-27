package ru.otus.btree.lib.v1.btree;

/**
 * Entity class for storing PageManager state.
 * Used for serialization and deserialization of page allocation information.
 */
public class PageManagerEntity {
    private long id;
    private int size;
    private boolean used;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
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

    /**
     * Serializes a PageManagerEntity to a byte array.
     *
     * @param entity the entity to serialize
     * @return byte array containing serialized data
     */
    public static byte[] serialize(PageManagerEntity entity) {
        if (entity == null) {
            return new byte[0];
        }

        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
             java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {

            dos.writeLong(entity.id);
            dos.writeInt(entity.size);
            dos.writeBoolean(entity.used);

            dos.flush();
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to serialize PageManagerEntity", e);
        }
    }

    /**
     * Deserializes a byte array to a PageManagerEntity.
     *
     * @param data the byte array to deserialize
     * @return the deserialized PageManagerEntity, or null if data is null or empty
     */
    public static PageManagerEntity deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(data);
             java.io.DataInputStream dis = new java.io.DataInputStream(bais)) {

            PageManagerEntity entity = new PageManagerEntity();
            entity.id = dis.readLong();
            entity.size = dis.readInt();
            entity.used = dis.readBoolean();

            return entity;
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to deserialize PageManagerEntity", e);
        }
    }
}
