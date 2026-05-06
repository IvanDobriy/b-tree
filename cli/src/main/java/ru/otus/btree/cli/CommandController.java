package ru.otus.btree.cli;

import ru.otus.btree.cli.command.ICommand;
import ru.otus.btree.command.Interaction;

public class CommandController {

    private ICommand findCommand(String command){
        throw new RuntimeException("not yet created");
    }

    public void handle(String msg, Interaction interaction){
        ICommand command = findCommand(msg);
        command.execute(interaction);
    }
}
