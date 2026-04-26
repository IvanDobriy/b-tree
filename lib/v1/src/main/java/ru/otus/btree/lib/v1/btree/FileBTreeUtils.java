package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.EType;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FileBTreeUtils {

    public static byte[] serialize(Element element) {
        if (element == null) {
            return new byte[0];
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            // Write name length and name
            String name = element.getName();
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(nameBytes.length);
            dos.write(nameBytes);

            // Write type
            EType type = element.getType();
            dos.writeInt(type.getType());

            // Write value based on type
            Object value = element.getValue();
            if (type == EType.STRING) {
                if (value == null) {
                    dos.writeInt(-1);
                } else {
                    String strValue = (String) value;
                    byte[] valueBytes = strValue.getBytes(StandardCharsets.UTF_8);
                    dos.writeInt(valueBytes.length);
                    dos.write(valueBytes);
                }
            } else if (type == EType.INTEGER) {
                if (value == null) {
                    dos.writeInt(0);
                } else {
                    dos.writeInt((Integer) value);
                }
            } else {
                dos.writeInt(-1);
            }

            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Element", e);
        }
    }

    public static Element deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            // Read name
            int nameLength = dis.readInt();
            byte[] nameBytes = new byte[nameLength];
            dis.readFully(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);

            // Read type
            int typeCode = dis.readInt();
            EType type = EType.STRING.getType() == typeCode ? EType.STRING : EType.INTEGER;

            // Read value based on type
            Object value = null;
            if (type == EType.STRING) {
                int valueLength = dis.readInt();
                if (valueLength >= 0) {
                    byte[] valueBytes = new byte[valueLength];
                    dis.readFully(valueBytes);
                    value = new String(valueBytes, StandardCharsets.UTF_8);
                }
            } else if (type == EType.INTEGER) {
                value = dis.readInt();
            }

            return new Element(name, type, value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize Element", e);
        }
    }
}
