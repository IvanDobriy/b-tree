package ru.otus.btree.lib.v1.btree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PageManagerHeaderTest {

    @Test
    public void testSerializeDeserialize() {
        PageManagerHeader original = new PageManagerHeader();
        original.setSize(1024L);

        byte[] serialized = PageManagerHeader.serialize(original);
        PageManagerHeader deserialized = PageManagerHeader.deserialize(serialized);

        assertNotNull(deserialized);
        assertEquals(1024L, deserialized.getSize());
    }

    @Test
    public void testSerializeNullHeader() {
        byte[] serialized = PageManagerHeader.serialize(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullData() {
        PageManagerHeader result = PageManagerHeader.deserialize(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        PageManagerHeader result = PageManagerHeader.deserialize(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testRoundTripMultipleSizes() {
        long[] sizes = {0L, 1L, 1024L, Long.MAX_VALUE};

        for (long size : sizes) {
            PageManagerHeader original = new PageManagerHeader();
            original.setSize(size);

            byte[] serialized = PageManagerHeader.serialize(original);
            PageManagerHeader deserialized = PageManagerHeader.deserialize(serialized);

            assertNotNull(deserialized);
            assertEquals(size, deserialized.getSize());
        }
    }
}
