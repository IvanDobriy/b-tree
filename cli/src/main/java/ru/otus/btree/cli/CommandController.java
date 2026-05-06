package ru.otus.btree.cli;

import ru.otus.btree.cli.command.GatewayCommand;
import ru.otus.btree.cli.command.GreetingCommand;
import ru.otus.btree.cli.command.ICommand;
import ru.otus.btree.command.Interaction;
import ru.otus.btree.data.storage.Storage;
import ru.otus.btree.domain.IStorage;

import java.nio.file.Path;

public class CommandController {
    private final ICommandLine commandLine;
    private ICommand currentCommand;
    private final Interaction interaction;
    private final IStorage storage;

    CommandController(){
        this.commandLine = CommandLineFactory.newCommandLine();
        this.storage = new Storage(Path.of("."));
        this.currentCommand = new GreetingCommand(new GatewayCommand(storage));
        interaction = new CliInteraction();
    }
    private class CliInteraction implements Interaction {

        @Override
        public String read() {
            return commandLine.readLine();
        }

        @Override
        public void write(String data) {
            commandLine.writeMsg(data);
        }
    }

    public void run() {
        while (currentCommand != null){
            currentCommand = currentCommand.execute(interaction);
        }
    }
}
