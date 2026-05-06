package ru.otus.btree.cli.command;

import ru.otus.btree.cases.CreateIndexUseCase;
import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;

import java.util.Objects;

public class CreateIndexCommand implements ICommand {
    private final ICommand nextCommand;
    private final CreateIndexUseCase useCase;

    public CreateIndexCommand(ICommand nextCommand, IStorage storage) {
        this.nextCommand = Objects.requireNonNull(nextCommand, "nextCommand is null");
        this.useCase = new CreateIndexUseCase(Objects.requireNonNull(storage, "storage is null"));
    }

    @Override
    public ICommand execute(Interaction interaction) {
        useCase.execute(interaction);
        return nextCommand;
    }
}
