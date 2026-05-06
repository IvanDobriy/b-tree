package ru.otus.btree.data.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class StorageIndexHeaderTest {

    @Test
    public void testSerializeDeserialize() {
        StorageIndexHeader original = new StorageIndexHeader();
        original.setSize(1024);

        byte[] serialized = StorageIndexHeader.serialize(original);
        StorageIndexHeader deserialized = StorageIndexHeader.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(1024, deserialized.getSize());
    }

    @Test
    public void testSerializeNullHeader() {
        byte[] serialized = StorageIndexHeader.serialize(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullData() {
        StorageIndexHeader result = StorageIndexHeader.deserialize(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        StorageIndexHeader result = StorageIndexHeader.deserialize(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testRoundTripMultipleSizes() {
        int[] sizes = {0, 1, 1024, Integer.MAX_VALUE};

        for (int size : sizes) {
            StorageIndexHeader original = new StorageIndexHeader();
            original.setSize(size);

            byte[] serialized = StorageIndexHeader.serialize(original);
            StorageIndexHeader deserialized = StorageIndexHeader.deserialize(serialized);

            assertNotNull(deserialized);
            assertEquals(size, deserialized.getSize());
        }
    }
}
