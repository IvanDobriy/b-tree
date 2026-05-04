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
        original.setId(7);

        byte[] serialized = StorageManagerEntity.serialize(original);
        StorageManagerEntity deserialized = StorageManagerEntity.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(4096L, deserialized.getPosition());
        assertEquals(4096, deserialized.getSize());
        assertTrue(deserialized.isUsed());
        assertEquals(7, deserialized.getId());
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
        original.setId(42);

        byte[] serialized = StorageManagerEntity.serialize(original);
        StorageManagerEntity deserialized = StorageManagerEntity.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(8192L, deserialized.getPosition());
        assertEquals(4096, deserialized.getSize());
        assertFalse(deserialized.isUsed());
        assertEquals(42, deserialized.getId());
    }

    @Test
    public void testRoundTripMultipleEntities() {
        StorageManagerEntity[] entities = {
            createEntity(0L, 4096, true, 1),
            createEntity(4096L, 4096, false, 2),
            createEntity(8192L, 8192, true, 3),
            createEntity(Long.MAX_VALUE, Integer.MAX_VALUE, false, Integer.MAX_VALUE)
        };

        for (StorageManagerEntity original : entities) {
            byte[] serialized = StorageManagerEntity.serialize(original);
            StorageManagerEntity deserialized = StorageManagerEntity.deserialize(serialized);

            assertNotNull(deserialized);
            assertEquals(original.getPosition(), deserialized.getPosition());
            assertEquals(original.getSize(), deserialized.getSize());
            assertEquals(original.isUsed(), deserialized.isUsed());
            assertEquals(original.getId(), deserialized.getId());
        }
    }

    @Test
    public void testConstructorWithParameters() {
        StorageManagerEntity entity = new StorageManagerEntity(123L, 456, true, 0);
        assertEquals(123L, entity.getPosition());
        assertEquals(456, entity.getSize());
        assertTrue(entity.isUsed());
        assertEquals(0, entity.getId());
    }

    @Test
    public void testConstructorWithIdParameter() {
        StorageManagerEntity entity = new StorageManagerEntity(123L, 456, true, 99);
        assertEquals(123L, entity.getPosition());
        assertEquals(456, entity.getSize());
        assertTrue(entity.isUsed());
        assertEquals(99, entity.getId());
    }

    private StorageManagerEntity createEntity(long position, int size, boolean used, int id) {
        StorageManagerEntity entity = new StorageManagerEntity();
        entity.setPosition(position);
        entity.setSize(size);
        entity.setUsed(used);
        entity.setId(id);
        return entity;
    }
}
