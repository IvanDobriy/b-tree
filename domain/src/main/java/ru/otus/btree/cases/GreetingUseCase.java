package ru.otus.btree.cases;

import ru.otus.btree.command.Interaction;

public class GreetingUseCase {
    public void execute(Interaction interaction) {
        interaction.write("Welcome to B-Tree CLI — a file-persisted B-tree storage application.");
        interaction.write("Type 'help' to see the list of available commands.");
    }
}
