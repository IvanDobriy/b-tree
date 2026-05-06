package ru.otus.btree.cli;

public class CommandLineFactory {
    public static ICommandLine newCommandLine() {
        return new CommandLineV1();
    }
}
