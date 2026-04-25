package ru.otus.btree.cli;

import java.util.logging.Logger;

public class BTreeCLI {
    private Logger logger = Logger.getLogger(this.getClass().getName());
    private void run(String[] args){
        logger.info("hello, world");
    }

    public static void main(String[] args) {
        BTreeCLI app = new BTreeCLI();
        app.run(args);
    }
}
