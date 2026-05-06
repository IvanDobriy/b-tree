package ru.otus.btree.cli.command;

import ru.otus.btree.command.Interaction;
import ru.otus.btree.lib.api.hash.IHashTable;
import ru.otus.btree.lib.v1.btree.StringHasher;
import ru.otus.btree.lib.v1.hash.OpenAddressHashTable;

public class GatewayCommand implements ICommand {
    private IHashTable<String, ICommand> commandTable;

    public GatewayCommand() {
        commandTable = new OpenAddressHashTable<>(new StringHasher(), 10, 1);
        commandTable.insert("help", new HelpCommand(this));
        commandTable.insert("exit", new ExitCommand());
    }

    @Override
    public ICommand execute(Interaction interaction) {
        interaction.write("введите команду:");
        String data = interaction.read();
        ICommand command = commandTable.find(data);
        if(command == null){
            interaction.write(String.format("command with name: %s not found, use 'help'", data));
            return this;
        }
        return command;
    }
}
