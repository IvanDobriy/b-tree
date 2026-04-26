package ru.otus.btree.lib.v1.btree;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.EType;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EntityTest {

    @Test
    public void testSetAndGet() {
        Entity entity = new Entity();
        Element element = new Element("testKey", EType.STRING, "testValue");

        entity.set(element);
        Element retrieved = entity.get("testKey");

        assertNotNull(retrieved);
        assertEquals("testKey", retrieved.getName());
        assertEquals(EType.STRING, retrieved.getType());
        assertEquals("testValue", retrieved.getValue());
    }

    @Test
    public void testGetNonExistentElement() {
        Entity entity = new Entity();

        Element result = entity.get("nonexistent");

        assertNull(result);
    }

    @Test
    public void testUpdateExistingElement() {
        Entity entity = new Entity();
        Element element1 = new Element("key1", EType.STRING, "value1");
        Element element2 = new Element("key1", EType.INTEGER, 42);

        entity.set(element1);
        entity.set(element2);

        Element retrieved = entity.get("key1");

        assertNotNull(retrieved);
        assertEquals("key1", retrieved.getName());
        assertEquals(EType.INTEGER, retrieved.getType());
        assertEquals(42, retrieved.getValue());
        assertEquals(1, entity.size());
    }

    @Test
    public void testSize() {
        Entity entity = new Entity();

        assertEquals(0, entity.size());

        entity.set(new Element("key1", EType.STRING, "value1"));
        assertEquals(1, entity.size());

        entity.set(new Element("key2", EType.INTEGER, 100));
        assertEquals(2, entity.size());

        entity.set(new Element("key3", EType.STRING, "value3"));
        assertEquals(3, entity.size());
    }

    @Test
    public void testIsEmpty() {
        Entity entity = new Entity();

        assertTrue(entity.isEmpty());

        entity.set(new Element("key1", EType.STRING, "value1"));

        assertFalse(entity.isEmpty());
    }

    @Test
    public void testMultipleElements() {
        Entity entity = new Entity();

        Element element1 = new Element("key1", EType.STRING, "value1");
        Element element2 = new Element("key2", EType.INTEGER, 42);
        Element element3 = new Element("key3", EType.STRING, "value3");

        entity.set(element1);
        entity.set(element2);
        entity.set(element3);

        assertEquals(3, entity.size());

        assertEquals("value1", entity.get("key1").getValue());
        assertEquals(42, entity.get("key2").getValue());
        assertEquals("value3", entity.get("key3").getValue());
    }

    @Test
    public void testSetNullElementThrowsException() {
        Entity entity = new Entity();

        assertThrows(NullPointerException.class, () -> entity.set(null));
    }

    @Test
    public void testIntegerTypeElement() {
        Entity entity = new Entity();
        Element element = new Element("intKey", EType.INTEGER, 999);

        entity.set(element);
        Element retrieved = entity.get("intKey");

        assertNotNull(retrieved);
        assertEquals(EType.INTEGER, retrieved.getType());
        assertEquals(999, retrieved.getValue());
    }

    @Test
    public void testElementWithNullValue() {
        Entity entity = new Entity();
        Element element = new Element("nullValueKey", EType.STRING, null);

        entity.set(element);
        Element retrieved = entity.get("nullValueKey");

        assertNotNull(retrieved);
        assertEquals("nullValueKey", retrieved.getName());
        assertNull(retrieved.getValue());
    }
}
