package ru.otus.btree.lib.api.array;

public interface IArray<T> {
    void set(int index, T item);
    void add( int index, T item);

    T remove(int index);


    T get(int index);

    int size();
}
