package ru.otus.btree.cli.command;

import ru.otus.btree.command.Interaction;

import java.util.Objects;

public class GreetingCommand implements ICommand{
    private final ICommand nextCommand;
    public GreetingCommand(ICommand nextCommand){
        this.nextCommand = Objects.requireNonNull(nextCommand, "next command is null");
    }

    @Override
    public ICommand execute(Interaction interaction) {
        interaction.write("Hi, this is cli");
        return nextCommand;
    }
}
