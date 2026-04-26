package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.crc.ICrc16;
import ru.otus.btree.lib.v1.crc.Crc16;
import ru.otus.btree.lib.v1.hash.IHasher;

import java.nio.charset.StandardCharsets;

public class StringHasher implements IHasher<String> {
    private final ICrc16 crc16;

    public StringHasher() {
        this.crc16 = new Crc16();
    }

    public StringHasher(ICrc16 crc16) {
        this.crc16 = crc16;
    }

    @Override
    public long execute(String key) {
        if (key == null) {
            return 0;
        }
        byte[] data = key.getBytes(StandardCharsets.UTF_8);
        return crc16.calculate(data);
    }
}
