package ru.otus.btree.lib.v1.btree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PageManagerEntityTest {

    @Test
    public void testSerializeDeserialize() {
        PageManagerEntity original = new PageManagerEntity();
        original.setId(4096L);
        original.setSize(4096);
        original.setUsed(true);

        byte[] serialized = PageManagerEntity.serialize(original);
        PageManagerEntity deserialized = PageManagerEntity.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(4096L, deserialized.getId());
        assertEquals(4096, deserialized.getSize());
        assertTrue(deserialized.isUsed());
    }

    @Test
    public void testSerializeNullEntity() {
        byte[] serialized = PageManagerEntity.serialize(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullData() {
        PageManagerEntity result = PageManagerEntity.deserialize(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        PageManagerEntity result = PageManagerEntity.deserialize(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testSerializeDeserializeUnusedPage() {
        PageManagerEntity original = new PageManagerEntity();
        original.setId(8192L);
        original.setSize(4096);
        original.setUsed(false);

        byte[] serialized = PageManagerEntity.serialize(original);
        PageManagerEntity deserialized = PageManagerEntity.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(8192L, deserialized.getId());
        assertEquals(4096, deserialized.getSize());
        assertFalse(deserialized.isUsed());
    }

    @Test
    public void testRoundTripMultipleEntities() {
        PageManagerEntity[] entities = {
            createEntity(0L, 4096, true),
            createEntity(4096L, 4096, false),
            createEntity(8192L, 8192, true),
            createEntity(Long.MAX_VALUE, Integer.MAX_VALUE, false)
        };

        for (PageManagerEntity original : entities) {
            byte[] serialized = PageManagerEntity.serialize(original);
            PageManagerEntity deserialized = PageManagerEntity.deserialize(serialized);

            assertNotNull(deserialized);
            assertEquals(original.getId(), deserialized.getId());
            assertEquals(original.getSize(), deserialized.getSize());
            assertEquals(original.isUsed(), deserialized.isUsed());
        }
    }

    private PageManagerEntity createEntity(long id, int size, boolean used) {
        PageManagerEntity entity = new PageManagerEntity();
        entity.setId(id);
        entity.setSize(size);
        entity.setUsed(used);
        return entity;
    }
}
