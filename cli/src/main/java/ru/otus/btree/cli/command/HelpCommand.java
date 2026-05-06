package ru.otus.btree.cli.command;

import ru.otus.btree.cases.GetHelpUseCase;
import ru.otus.btree.command.Interaction;

import java.util.Objects;

public class HelpCommand implements ICommand {
    private final GetHelpUseCase getHelpUseCase;
    private final ICommand nextCommand;

    public HelpCommand(ICommand nextCommand) {
        this.nextCommand = Objects.requireNonNull(nextCommand, "nextCommand is null");
        getHelpUseCase = new GetHelpUseCase();
    }

    @Override
    public ICommand execute(Interaction interaction) {
        getHelpUseCase.execute(interaction);
        return nextCommand;
    }
}
