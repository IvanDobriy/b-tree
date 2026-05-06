package ru.otus.btree.cli;

import java.io.PrintStream;
import java.util.Scanner;

public class CommandLineV1 implements ICommandLine{
    private final Scanner scanner;
    private final PrintStream printer;

    public CommandLineV1(){
        scanner = new Scanner(System.in);
        printer = System.out;

    }

    @Override
    public String readLine() {
        return scanner.nextLine();
    }

    @Override
    public void writeMsg(String msg) {
        printer.println(msg);
    }
}
