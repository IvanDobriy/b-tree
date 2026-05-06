package ru.otus.btree.cli.command;

import ru.otus.btree.cases.GreetingUseCase;
import ru.otus.btree.command.Interaction;

import java.util.Objects;

public class GreetingCommand implements ICommand {
    private final ICommand nextCommand;
    private final GreetingUseCase greetingUseCase;

    public GreetingCommand(ICommand nextCommand) {
        this.nextCommand = Objects.requireNonNull(nextCommand, "next command is null");
        this.greetingUseCase = new GreetingUseCase();
    }

    @Override
    public ICommand execute(Interaction interaction) {
        greetingUseCase.execute(interaction);
        return nextCommand;
    }
}
