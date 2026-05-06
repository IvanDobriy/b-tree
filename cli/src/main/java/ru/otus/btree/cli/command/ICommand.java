package ru.otus.btree.cli.command;

import ru.otus.btree.command.Interaction;

public interface ICommand {
    ICommand execute(Interaction interaction);
}
