package ru.otus.btree.cli.command;

import ru.otus.btree.cases.GetEntityUseCase;
import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;

import java.util.Objects;

public class GetEntityCommand implements ICommand {
    private final ICommand nextCommand;
    private final GetEntityUseCase useCase;

    public GetEntityCommand(ICommand nextCommand, IStorage storage) {
        this.nextCommand = Objects.requireNonNull(nextCommand, "nextCommand is null");
        this.useCase = new GetEntityUseCase(Objects.requireNonNull(storage, "storage is null"));
    }

    @Override
    public ICommand execute(Interaction interaction) {
        useCase.execute(interaction);
        return nextCommand;
    }
}
