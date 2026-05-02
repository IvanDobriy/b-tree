# b-tree

## Project Overview

This is an educational Java project from the OTUS course implementing a **file-persisted B-tree** data structure from scratch. The project demonstrates low-level data structure implementation, custom serialization, and page-based file I/O using Java NIO.

Key characteristics:
- **Language**: Java 11
- **Build Tool**: Gradle 7.6.4 with Kotlin DSL (`*.gradle.kts`)
- **Test Framework**: JUnit 5.9.3
- **Package Root**: `ru.otus.btree.*`
- **Runtime Dependencies**: None (only JUnit for tests)

## Module Structure

The project is a Gradle multi-project build with 5 modules:

| Module | Type | Description | Dependencies |
|--------|------|-------------|--------------|
| `cli` | Application | Entry point (`BTreeCLI`) — currently a minimal "hello, world" stub | `:domain` |
| `domain` | Library (`java-library`) | Empty placeholder module | — |
| `data` | Library (`java-library`) | Empty placeholder module | — |
| `lib:api` | Library (`java-library`) | Public API interfaces (`IBTree`, `IEntity`, `IArray`, `IHashTable`, `ICrc16`) | — |
| `lib:v1` | Library (`java-library`) | v1 implementation of the API: file-based B-tree, page manager, hash table, dynamic array, CRC-16 | `:lib:api` |

### Source Layout

Standard Maven/Gradle directory layout is used:
```
<module>/src/main/java/ru/otus/btree/...   # production code
<module>/src/test/java/ru/otus/btree/...   # test code
```

### Key Packages

- `ru.otus.btree.lib.api.btree` — B-tree API: `IBTree`, `IEntity`, `Element`, `EType`
- `ru.otus.btree.lib.api.array` — Dynamic array API: `IArray<T>`
- `ru.otus.btree.lib.api.hash` — Hash table API: `IHashTable<K,V>`
- `ru.otus.btree.lib.api.crc` — CRC API: `ICrc16`
- `ru.otus.btree.lib.v1.btree` — Implementation: `FileBTree`, `FileBTreeNode`, `PageManager`, `PageManagerList`, `PageManagerHeader`, `PageManagerEntity`, `Entity`, `FileBTreeUtils`, `StringHasher`
- `ru.otus.btree.lib.v1.array` — Implementation: `SingleArray<T>`, `ArrayUtils`
- `ru.otus.btree.lib.v1.hash` — Implementation: `OpenAddressHashTable<K,V>`, `IHasher<K>`
- `ru.otus.btree.lib.v1.crc` — Implementation: `Crc16`
- `ru.otus.btree.cli` — CLI entry point: `BTreeCLI`

## Technology Stack

- **Java Toolchain**: 11 (configured in every `build.gradle.kts`)
- **Gradle Wrapper**: `gradle/wrapper/gradle-wrapper.properties` → Gradle 7.6.4
- **JUnit**: 5.9.3 (`junit-jupiter`)
- **I/O**: `java.nio.channels.FileChannel`, `java.io.DataOutputStream` / `DataInputStream`
- **Encoding**: UTF-8 for string serialization

## Build and Test Commands

```bash
# Build all modules
./gradlew build

# Run all tests
./gradlew test

# Run tests for a specific module
./gradlew :lib:v1:test

# Run the CLI application
./gradlew :cli:run

# Clean build outputs
./gradlew clean
```

### Known Build Issues

As of the latest source state, the project **does not compile**:
- `lib/v1/src/main/java/ru/otus/btree/lib/v1/btree/FileBTree.java:39`  
  `return null;` is used in `search(Element)`, which is declared to return `long` in `IBTree`.

To fix it, change the return type or return value in `FileBTree.search` so that primitive `long` is handled correctly.

## Architecture and Design

### File-Based B-Tree

The core implementation (`lib:v1`) stores a B-tree on disk using two `FileChannel`s:
1. **Page manager channel** — tracks page allocation metadata (`PageManager`, `PageManagerList`)
2. **Node channel** — stores serialized B-tree nodes (`FileBTreeNode`)

- **Page size**: 4096 bytes (`PAGE_SIZE` constant)
- **Node serialization**: custom binary format via `FileBTreeUtils`
  - `Element` → `(nameLength, nameBytes, typeCode, valueLength/valueBytes/valueInt)`
  - `FileBTreeNode` → `(pageId, degree, isLeaf, parentPageId, keyCount, [keys...], childCount, [children...])`
- **Root tracking**: root is expected at page ID 0; `IOnRootChanged` callback updates the in-memory root reference

### Data Structures Implemented from Scratch

| Structure | File | Notes |
|-----------|------|-------|
| Dynamic array | `SingleArray<T>` | Wraps `Object[]`; grows/shrinks on `add`/`remove` using `System.arraycopy` |
| Hash table | `OpenAddressHashTable<K,V>` | Open addressing with linear probing; rehashes when load factor > 0.7 |
| String hasher | `StringHasher` | CRC-16 over UTF-8 bytes |
| CRC-16 | `Crc16` | Polynomial `0xA001`, initial `0xFFFF` |

### Entity Model

- `Element` — a named, typed value (`EType.STRING` or `EType.INTEGER`)
- `Entity` — a map of `Element`s backed by `OpenAddressHashTable`
- `IEntity.toArray()` is currently stubbed (`return null`)

## Testing Strategy

- **Framework**: JUnit 5 (Jupiter)
- **Lifecycle**: Most test classes use `@TestInstance(TestInstance.Lifecycle.PER_CLASS)`
- **Temporary files**: Tests use `@TempDir Path tempDir` and create `RandomAccessFile` / `FileChannel` pairs for disk-based tests
- **Test modules**:
  - `lib:v1` has the bulk of tests: `FileBTreeNodeTest`, `PageManagerTest`, `PageManagerListTest`, `PageManagerHeaderTest`, `PageManagerEntityTest`, `FileBTreeUtilsTest`, `EntityTest`, `OpenAddressHashTableTest`
  - `cli` has a single smoke test: `SomeTest`
  - `lib:api` has no tests

### Running Tests

```bash
./gradlew test
```

Reports are generated under `<module>/build/reports/tests/test/index.html`.

## Code Style Guidelines

- **Language**: English for code and primary documentation; occasional Russian strings may appear in test data.
- **Naming**:
  - Interfaces are prefixed with `I`: `IBTree`, `IEntity`, `IArray`
  - Implementations drop the prefix: `FileBTree`, `Entity`, `SingleArray`
- **Null safety**: `Objects.requireNonNull(...)` is used defensively for constructor and method arguments.
- **Visibility**: Fields are typically `private`; getters/setters are provided.
- **Exceptions**: I/O errors in serialization are wrapped in `RuntimeException`.
- **Serialization**: All persistent classes provide static `serialize(...)` / `deserialize(...)` methods (or instance methods in `FileBTreeUtils`).

## Security Considerations

- No network, authentication, or cryptography is involved.
- File I/O operates on user-provided file paths (via `FileChannel` construction in tests and future CLI usage).
- The CRC-16 implementation is for hashing/addressing only, not for data integrity security.

## Development Notes for Agents

1. **Empty modules**: `domain` and `data` have no Java source files. They are placeholders for future expansion.
2. **Unimplemented features**:
   - `FileBTree.delete(...)` throws `RuntimeException("not yet implemented")`.
   - `FileBTree.search(...)` has the compilation bug mentioned above.
   - `Entity.toArray()` returns `null`.
3. **Page management**: `PageManager` reuses deleted pages when available; otherwise it appends new pages at the end of the file. `PageManagerList` handles reading/writing across 4096-byte page boundaries.
4. **Modifying the B-tree**: Any change to node structure or serialization must be reflected in `FileBTreeUtils` and corresponding tests.
