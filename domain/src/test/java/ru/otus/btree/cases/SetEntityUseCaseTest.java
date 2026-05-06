package ru.otus.btree.cases;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import ru.otus.btree.command.Interaction;
import ru.otus.btree.domain.IStorage;
import ru.otus.btree.lib.api.btree.EType;
import ru.otus.btree.lib.api.btree.Element;
import ru.otus.btree.lib.api.btree.IEntity;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SetEntityUseCaseTest {

    @Test
    public void testSuccessOneField() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name", "string", "John", "!done");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<IEntity> entityCaptor = ArgumentCaptor.forClass(IEntity.class);
        verify(storage).setEntity(nameCaptor.capture(), entityCaptor.capture());

        assertEquals("users", nameCaptor.getValue());
        IEntity entity = entityCaptor.getValue();
        Element element = entity.get("name");
        assertNotNull(element);
        assertEquals(EType.STRING, element.getType());
        assertEquals("John", element.getValue());

        assertWritten(interaction, "Entity saved successfully");
    }

    @Test
    public void testSuccessMultipleFields() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn(
                "users",
                "name", "string", "John",
                "age", "integer", "30",
                "!done"
        );

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        ArgumentCaptor<IEntity> entityCaptor = ArgumentCaptor.forClass(IEntity.class);
        verify(storage).setEntity(eq("users"), entityCaptor.capture());

        IEntity entity = entityCaptor.getValue();
        Element nameElement = entity.get("name");
        assertNotNull(nameElement);
        assertEquals(EType.STRING, nameElement.getType());
        assertEquals("John", nameElement.getValue());

        Element ageElement = entity.get("age");
        assertNotNull(ageElement);
        assertEquals(EType.INTEGER, ageElement.getType());
        assertEquals(30, ageElement.getValue());

        assertWritten(interaction, "Entity saved successfully");
    }

    @Test
    public void testCancelAtStorageName() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("!stop");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).setEntity(any(), any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testRetryBlankStorageNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("", "  ", "users", "name", "string", "John", "!done");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).setEntity(eq("users"), any());
        assertWritten(interaction, "Error: name cannot be empty");
    }

    @Test
    public void testRetryNullStorageNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn(null, "users", "name", "string", "John", "!done");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).setEntity(eq("users"), any());
        assertWritten(interaction, "Error: input is null");
    }

    @Test
    public void testCancelAtFieldName() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "!stop");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).setEntity(any(), any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testEmptyFieldNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "", "  ", "name", "string", "John", "!done");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).setEntity(eq("users"), any());
        assertWritten(interaction, "Error: field name cannot be empty");
    }

    @Test
    public void testNullFieldNameThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", null, "name", "string", "John", "!done");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).setEntity(eq("users"), any());
        assertWritten(interaction, "Error: input is null");
    }

    @Test
    public void testCancelAtFieldType() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name", "!stop");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).setEntity(any(), any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testUnknownTypeThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name", "foo", "string", "John", "!done");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage).setEntity(eq("users"), any());
        assertWritten(interaction, "Error: unknown type 'foo'");
    }

    @Test
    public void testCancelAtFieldValue() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "name", "string", "!stop");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).setEntity(any(), any());
        assertWritten(interaction, "Operation cancelled.");
    }

    @Test
    public void testInvalidIntegerThenSuccess() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "age", "integer", "abc", "30", "!done");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        ArgumentCaptor<IEntity> entityCaptor = ArgumentCaptor.forClass(IEntity.class);
        verify(storage).setEntity(eq("users"), entityCaptor.capture());

        IEntity entity = entityCaptor.getValue();
        Element ageElement = entity.get("age");
        assertNotNull(ageElement);
        assertEquals(EType.INTEGER, ageElement.getType());
        assertEquals(30, ageElement.getValue());

        assertWritten(interaction, "Error: invalid integer 'abc'");
    }

    @Test
    public void testNoFieldsEntered() {
        IStorage storage = mock(IStorage.class);
        Interaction interaction = mock(Interaction.class);
        when(interaction.read()).thenReturn("users", "!done");

        SetEntityUseCase useCase = new SetEntityUseCase(storage);
        useCase.execute(interaction);

        verify(storage, never()).setEntity(any(), any());
        assertWritten(interaction, "Error: entity must have at least one field");
    }

    private void assertWritten(Interaction interaction, String text) {
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(interaction, atLeastOnce()).write(captor.capture());
        List<String> messages = captor.getAllValues();
        assertTrue(messages.stream().anyMatch(m -> m.contains(text)),
                "Expected interaction to contain: " + text);
    }
}
