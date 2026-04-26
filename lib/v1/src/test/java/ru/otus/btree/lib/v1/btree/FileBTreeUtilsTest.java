package ru.otus.btree.lib.v1.btree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.EType;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileBTreeUtilsTest {

    @Test
    public void testSerializeDeserializeStringElement() {
        Element original = new Element("testKey", EType.STRING, "testValue");
        byte[] serialized = FileBTreeUtils.serializeElement(original);
        Element deserialized = FileBTreeUtils.deserializeElement(serialized);

        assertNotNull(deserialized);
        assertEquals("testKey", deserialized.getName());
        assertEquals(EType.STRING, deserialized.getType());
        assertEquals("testValue", deserialized.getValue());
    }

    @Test
    public void testSerializeDeserializeIntegerElement() {
        Element original = new Element("intKey", EType.INTEGER, 42);
        byte[] serialized = FileBTreeUtils.serializeElement(original);
        Element deserialized = FileBTreeUtils.deserializeElement(serialized);

        assertNotNull(deserialized);
        assertEquals("intKey", deserialized.getName());
        assertEquals(EType.INTEGER, deserialized.getType());
        assertEquals(42, deserialized.getValue());
    }

    @Test
    public void testSerializeNullElement() {
        byte[] serialized = FileBTreeUtils.serializeElement(null);
        assertEquals(0, serialized.length);
    }

    @Test
    public void testDeserializeNullData() {
        Element result = FileBTreeUtils.deserializeElement(null);
        assertNull(result);
    }

    @Test
    public void testDeserializeEmptyData() {
        Element result = FileBTreeUtils.deserializeElement(new byte[0]);
        assertNull(result);
    }

    @Test
    public void testSerializeDeserializeStringWithNullValue() {
        Element original = new Element("nullStringKey", EType.STRING, null);
        byte[] serialized = FileBTreeUtils.serializeElement(original);
        Element deserialized = FileBTreeUtils.deserializeElement(serialized);

        assertNotNull(deserialized);
        assertEquals("nullStringKey", deserialized.getName());
        assertEquals(EType.STRING, deserialized.getType());
        assertNull(deserialized.getValue());
    }

    @Test
    public void testSerializeDeserializeIntegerWithNullValue() {
        Element original = new Element("nullIntKey", EType.INTEGER, null);
        byte[] serialized = FileBTreeUtils.serializeElement(original);
        Element deserialized = FileBTreeUtils.deserializeElement(serialized);

        assertNotNull(deserialized);
        assertEquals("nullIntKey", deserialized.getName());
        assertEquals(EType.INTEGER, deserialized.getType());
        // null integer is serialized as 0
        assertEquals(0, deserialized.getValue());
    }

    @Test
    public void testSerializeDeserializeUtf8String() {
        Element original = new Element("ключ", EType.STRING, "значение");
        byte[] serialized = FileBTreeUtils.serializeElement(original);
        Element deserialized = FileBTreeUtils.deserializeElement(serialized);

        assertNotNull(deserialized);
        assertEquals("ключ", deserialized.getName());
        assertEquals("значение", deserialized.getValue());
    }

    @Test
    public void testRoundTripMultipleElements() {
        Element[] elements = {
            new Element("key1", EType.STRING, "value1"),
            new Element("key2", EType.INTEGER, 100),
            new Element("key3", EType.STRING, null),
            new Element("key4", EType.INTEGER, -999),
            new Element("key5", EType.STRING, "special!@#$%^&*()")
        };

        for (Element original : elements) {
            byte[] serialized = FileBTreeUtils.serializeElement(original);
            Element deserialized = FileBTreeUtils.deserializeElement(serialized);

            assertNotNull(deserialized);
            assertEquals(original.getName(), deserialized.getName());
            assertEquals(original.getType(), deserialized.getType());
            assertEquals(original.getValue(), deserialized.getValue());
        }
    }
}
