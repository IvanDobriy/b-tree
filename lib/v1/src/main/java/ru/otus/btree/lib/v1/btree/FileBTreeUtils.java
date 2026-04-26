package ru.otus.btree.lib.v1.btree;

import ru.otus.btree.lib.api.array.IArray;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.IEntity;
import ru.otus.btree.lib.v1.array.SingleArray;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FileBTreeUtils {

    public static byte[] serializeElement(Element element) {
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

    public static Element deserializeElement(byte[] data) {
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

    public static byte[] serializeEntity(IEntity entity) {
        if (entity == null) {
            return new byte[0];
        }

        // Entity doesn't provide direct access to all elements
        // We need to use a workaround: store elements as array of serialized elements
        if (!(entity instanceof Entity)) {
            throw new IllegalArgumentException("Entity must be an instance of ru.otus.btree.lib.v1.btree.Entity");
        }

        Entity concreteEntity = (Entity) entity;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            // Write number of elements
            dos.writeInt(concreteEntity.size());

            // Since Entity uses IHashTable internally and doesn't expose all elements,
            // we need to track elements during serialization
            // For now, we'll use a simple approach: serialize elements one by one
            // This requires Entity to provide access to all elements

            // Note: Current Entity implementation doesn't support iteration.
            // This is a limitation that requires either:
            // 1. Adding iterator/getAllElements to Entity
            // 2. Changing Entity to use IArray instead of IHashTable for storage
            // 3. Tracking keys separately

            // For now, we throw UnsupportedOperationException
            // until Entity provides iteration capability
            throw new UnsupportedOperationException("Entity serialization requires Entity to provide iteration over elements. " +
                    "Consider adding getAllElements() method to Entity class.");

        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Entity", e);
        }
    }

    public static IEntity deserializeEntity(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            // Read number of elements
            int elementCount = dis.readInt();

            Entity entity = new Entity();

            for (int i = 0; i < elementCount; i++) {
                // Read serialized element length
                int elementLength = dis.readInt();
                byte[] elementData = new byte[elementLength];
                dis.readFully(elementData);

                // Deserialize element
                Element element = deserializeElement(elementData);
                if (element != null) {
                    entity.set(element);
                }
            }

            return entity;

        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize Entity", e);
        }
    }
}
