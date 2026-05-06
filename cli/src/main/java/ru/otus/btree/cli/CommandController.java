package ru.otus.btree.cli;

import ru.otus.btree.cli.command.GatewayCommand;
import ru.otus.btree.cli.command.GreetingCommand;
import ru.otus.btree.cli.command.ICommand;
import ru.otus.btree.command.Interaction;

public class CommandController {
    private final ICommandLine commandLine;
    private ICommand currentCommand;
    private final Interaction interaction;

    CommandController(){
        this.commandLine = CommandLineFactory.newCommandLine();
        this.currentCommand = new GreetingCommand(new GatewayCommand());
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
