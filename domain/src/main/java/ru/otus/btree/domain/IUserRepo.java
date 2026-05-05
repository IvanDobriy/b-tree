package ru.otus.btree.domain;

import ru.otus.btree.lib.api.array.IArray;

public interface IUserRepo {
    IArray<User> getUserById(int id);
    IArray<User> getUserByName(String name);
    IArray<User> getUserByAge(int age);
    void createUser(User user);
}
