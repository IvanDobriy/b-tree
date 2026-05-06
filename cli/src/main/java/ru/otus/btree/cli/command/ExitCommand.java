package ru.otus.btree.cli.command;

import ru.otus.btree.command.Interaction;

public class ExitCommand implements ICommand{

    @Override
    public ICommand execute(Interaction interaction) {
        interaction.write("Executing exit command");
        return null;
    }
}
