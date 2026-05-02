package ru.otus.btree.lib.v1.array;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import ru.otus.btree.lib.api.array.IArray;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SingleArrayTest {

    @Test
    public void testCreateWithSize() {
        IArray<String> array = new SingleArray<>(5);
        assertEquals(5, array.size());
    }

    @Test
    public void testCreateWithArray() {
        Integer[] source = {1, 2, 3};
        IArray<Integer> array = new SingleArray<>(source);
        assertEquals(3, array.size());
        assertEquals(1, array.get(0));
        assertEquals(2, array.get(1));
        assertEquals(3, array.get(2));
    }

    @Test
    public void testCreateWithArrayThrowsOnNull() {
        assertThrows(NullPointerException.class, () -> new SingleArray<>((String[]) null));
    }

    @Test
    public void testSetAndGet() {
        IArray<String> array = new SingleArray<>(3);
        array.set(0, "a");
        array.set(1, "b");
        array.set(2, "c");

        assertEquals("a", array.get(0));
        assertEquals("b", array.get(1));
        assertEquals("c", array.get(2));
    }

    @Test
    public void testGetThrowsOnNegativeIndex() {
        IArray<String> array = new SingleArray<>(3);
        assertThrows(IllegalArgumentException.class, () -> array.get(-1));
    }

    @Test
    public void testGetThrowsOnOutOfRange() {
        IArray<String> array = new SingleArray<>(3);
        assertThrows(IllegalArgumentException.class, () -> array.get(3));
    }

    @Test
    public void testAddAtBeginning() {
        IArray<String> array = new SingleArray<>(new String[]{"b", "c"});
        array.add(0, "a");

        assertEquals(3, array.size());
        assertEquals("a", array.get(0));
        assertEquals("b", array.get(1));
        assertEquals("c", array.get(2));
    }

    @Test
    public void testAddInMiddle() {
        IArray<String> array = new SingleArray<>(new String[]{"a", "c"});
        array.add(1, "b");

        assertEquals(3, array.size());
        assertEquals("a", array.get(0));
        assertEquals("b", array.get(1));
        assertEquals("c", array.get(2));
    }

    @Test
    public void testAddAtEnd() {
        IArray<String> array = new SingleArray<>(new String[]{"a", "b"});
        array.add(2, "c");

        assertEquals(3, array.size());
        assertEquals("a", array.get(0));
        assertEquals("b", array.get(1));
        assertEquals("c", array.get(2));
    }

    @Test
    public void testAddBeyondCurrentSize() {
        IArray<String> array = new SingleArray<>(new String[]{"a"});
        array.add(3, "d");

        assertEquals(4, array.size());
        assertEquals("a", array.get(0));
        assertNull(array.get(1));
        assertNull(array.get(2));
        assertEquals("d", array.get(3));
    }

    @Test
    public void testAddThrowsOnNegativeIndex() {
        IArray<String> array = new SingleArray<>(3);
        assertThrows(IllegalArgumentException.class, () -> array.add(-1, "x"));
    }

    @Test
    public void testRemoveFromBeginning() {
        IArray<String> array = new SingleArray<>(new String[]{"a", "b", "c"});
        String removed = array.remove(0);

        assertEquals("a", removed);
        assertEquals(2, array.size());
        assertEquals("b", array.get(0));
        assertEquals("c", array.get(1));
    }

    @Test
    public void testRemoveFromMiddle() {
        IArray<String> array = new SingleArray<>(new String[]{"a", "b", "c"});
        String removed = array.remove(1);

        assertEquals("b", removed);
        assertEquals(2, array.size());
        assertEquals("a", array.get(0));
        assertEquals("c", array.get(1));
    }

    @Test
    public void testRemoveFromEnd() {
        IArray<String> array = new SingleArray<>(new String[]{"a", "b", "c"});
        String removed = array.remove(2);

        assertEquals("c", removed);
        assertEquals(2, array.size());
        assertEquals("a", array.get(0));
        assertEquals("b", array.get(1));
    }

    @Test
    public void testRemoveThrowsOnNegativeIndex() {
        IArray<String> array = new SingleArray<>(3);
        assertThrows(IllegalArgumentException.class, () -> array.remove(-1));
    }

    @Test
    public void testRemoveThrowsOnOutOfRange() {
        IArray<String> array = new SingleArray<>(3);
        assertThrows(IllegalArgumentException.class, () -> array.remove(3));
    }

    @Test
    public void testSizeAfterAddAndRemove() {
        IArray<Integer> array = new SingleArray<>(0);
        assertEquals(0, array.size());

        array.add(0, 10);
        assertEquals(1, array.size());

        array.add(1, 20);
        assertEquals(2, array.size());

        array.remove(0);
        assertEquals(1, array.size());
    }

    @Test
    public void testAddAndRemoveManyElements() {
        IArray<Integer> array = new SingleArray<>(0);
        int count = 100;

        for (int i = 0; i < count; i++) {
            array.add(i, i);
        }

        assertEquals(count, array.size());

        for (int i = 0; i < count; i++) {
            assertEquals(i, array.get(i));
        }

        for (int i = count - 1; i >= 0; i--) {
            assertEquals(i, array.remove(i));
        }

        assertEquals(0, array.size());
    }

    @Test
    public void addToArray(){
        IArray<Integer> array = new SingleArray<>(0);
        array.add(0, 1);
        array.add(1, 2);
        array.add(0, 3);
        array.add(0, 4);
        assertEquals(4, array.get(0));
        assertEquals(3, array.get(1));
        assertEquals(1, array.get(2));
        assertEquals(2, array.get(3));
    }
}
