package ru.otus.btree.lib.v1.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageManagerEntityTest {

    @Test
    public void testSerializeDeserialize() {
        StorageManagerEntity original = new StorageManagerEntity();
        original.setPosition(4096L);
        original.setSize(4096);
        original.setUsed(true);

        byte[] serialized = StorageManagerEntity.serialize(original);
        StorageManagerEntity deserialized = StorageManagerEntity.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(4096L, deserialized.getPosition());
        assertEquals(4096, deserialized.getSize());
        assertTrue(deserialized.isUsed());
    }

    @Test
    public void testSerializeNullEntity() {
        byte[] serialized = StorageManagerEntity.serialize(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullData() {
        StorageManagerEntity result = StorageManagerEntity.deserialize(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        StorageManagerEntity result = StorageManagerEntity.deserialize(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testSerializeDeserializeUnusedEntity() {
        StorageManagerEntity original = new StorageManagerEntity();
        original.setPosition(8192L);
        original.setSize(4096);
        original.setUsed(false);

        byte[] serialized = StorageManagerEntity.serialize(original);
        StorageManagerEntity deserialized = StorageManagerEntity.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(8192L, deserialized.getPosition());
        assertEquals(4096, deserialized.getSize());
        assertFalse(deserialized.isUsed());
    }

    @Test
    public void testRoundTripMultipleEntities() {
        StorageManagerEntity[] entities = {
            createEntity(0L, 4096, true),
            createEntity(4096L, 4096, false),
            createEntity(8192L, 8192, true),
            createEntity(Long.MAX_VALUE, Integer.MAX_VALUE, false)
        };

        for (StorageManagerEntity original : entities) {
            byte[] serialized = StorageManagerEntity.serialize(original);
            StorageManagerEntity deserialized = StorageManagerEntity.deserialize(serialized);

            assertNotNull(deserialized);
            assertEquals(original.getPosition(), deserialized.getPosition());
            assertEquals(original.getSize(), deserialized.getSize());
            assertEquals(original.isUsed(), deserialized.isUsed());
        }
    }

    @Test
    public void testConstructorWithParameters() {
        StorageManagerEntity entity = new StorageManagerEntity(123L, 456, true);
        assertEquals(123L, entity.getPosition());
        assertEquals(456, entity.getSize());
        assertTrue(entity.isUsed());
    }

    private StorageManagerEntity createEntity(long id, int size, boolean used) {
        StorageManagerEntity entity = new StorageManagerEntity();
        entity.setPosition(id);
        entity.setSize(size);
        entity.setUsed(used);
        return entity;
    }
}
