package ru.otus.btree.lib.api.crc;


class Crc16 implements ICrc16 {
    private final int initial;
    private final int mask;

    public Crc16() {
        this.initial = 0xffff;
        this.mask = 0xa001;
    }

    public int calculate(byte[] data) {
        int crc = initial;
        for (int i = 0; i < data.length; i++) {
            crc ^= (int) data[i];
            for (int j = 0; j < 8; j++) {
                if ((crc & 0x0001) != 0) {
                    crc >>= 1;
                    crc ^= mask;
                } else {
                    crc >>= 1;
                }
            }
        }
        return crc;
    }
}