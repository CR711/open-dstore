# Coding Style Guide

Based on existing dstore conventions and C++ best practices from similar storage engine projects (RocksDB, PostgreSQL, LevelDB).

## Naming

| Element | Convention | Example |
|---|---|---|
| Class / Struct | PascalCase | `BufMgr`, `Transaction`, `WalManager` |
| Method (public) | PascalCase | `Initialize()`, `GetSnapshotCsn()` |
| Method (private) | PascalCase | `PreCommit()`, `RecordCommit()` |
| Member variable | `m_` + camelCase | `m_pdbId`, `m_instance`, `m_snapshot` |
| Local variable | camelCase | `lockTable`, `baseBufDesc` |
| Constant (macro) | UPPER_SNAKE | `BLCKSZ`, `PAGE_HEADER_FMT` |
| Constant (constexpr) | UPPER_SNAKE | `BITS_PER_BYTE`, `HASH_INIT_PARTITION_COUNT` |
| Enum class | PascalCase type, UPPER_SNAKE values | `enum class WalLevel { WAL_LEVEL_MINIMAL }` |
| File name | `dstore_` + snake_case | `dstore_lock_mgr.cpp` |
| Macro | UPPER_SNAKE with project prefix | `DSTORE_`, `STORAGE_`, `PAGE_` |

### Getter/Setter/Checker

- Getter: `GetXxx()` (e.g., `GetBufferMgr()`, `GetPdbId()`)
- Setter: `SetXxx()` (e.g., `SetReadOnly()`, `SetCurrentXid()`)
- Boolean checker: `IsXxx()` or `HasXxx()` (e.g., `IsValid()`, `InTransaction()`)

## File Layout

### Header (.h)

```cpp
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: Brief description
 *
 * IDENTIFICATION
 *        storage/include/<subsystem>/dstore_<name>.h
 */

#ifndef DSTORE_<SUBSYSTEM>_<NAME>_H
#define DSTORE_<SUBSYSTEM>_<NAME>_H

#include <cstdint>                          // system headers first
#include "subsystem/dstore_dependency.h"    // project headers second

namespace DSTORE {

// forward declarations
class Foo;

// enums
enum class BarType { BAR_DEFAULT, BAR_SPECIAL };

// structs
struct BazContext {
    uint32_t field;
};

// class declaration
class MyClass : public BaseObject {
public:
    MyClass() = default;
    virtual ~MyClass() = default;
    DISALLOW_COPY_AND_MOVE(MyClass);

    RetStatus Initialize();
    void Destroy();

private:
    uint32_t m_count{0};
};

// inline functions
inline bool IsReady() { return true; }

// extern declarations
extern RetStatus GlobalFunction();

}  // namespace DSTORE

#endif  // DSTORE_<SUBSYSTEM>_<NAME>_H
```

### Source (.cpp)

```cpp
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: Brief description
 *
 * IDENTIFICATION
 *        storage/src/<subsystem>/dstore_<name>.cpp
 */

#include "<subsystem>/dstore_<name>.h"     // own header first

#include <algorithm>                        // system headers
#include "other/dstore_dependency.h"        // project headers

namespace DSTORE {

RetStatus MyClass::Initialize()
{
    // implementation
}

}  // namespace DSTORE
```

## Formatting

### Indentation & Whitespace
- 4 spaces per level, no tabs
- No trailing whitespace
- Single blank line between function definitions
- No blank line after opening brace or before closing brace

### Braces
- **Function definitions**: Allman style (brace on new line)
  ```cpp
  RetStatus Transaction::Start()
  {
      // body
  }
  ```
- **Control flow**: K&R style (brace on same line)
  ```cpp
  if (condition) {
      // body
  } else {
      // alternative
  }

  for (int i = 0; i < n; i++) {
      // body
  }
  ```
- **Class/struct**: brace on same line
  ```cpp
  class Foo : public Bar {
  public:
      // ...
  };
  ```
- Always use braces for control flow, even single-line bodies

### Pointers & References
- Star/ampersand near variable: `Type *ptr`, `Type &ref`
  ```cpp
  DataPage *localPage = nullptr;
  const Transaction &trx = GetCurrentTrx();
  ```

### Line Length
- Soft limit: 120 characters
- Hard limit: 150 characters
- Break long lines at logical boundaries (after comma, before operator)

## Error Handling

- Use `RetStatus` return codes, **never exceptions**
- Check returns with `STORAGE_FUNC_SUCC(ret)` / `STORAGE_FUNC_FAIL(ret)`
- Log errors with `ErrLog(DSTORE_ERROR, MODULE_XXX, ErrMsg(...))`
- Set errors with `storage_set_error()`
- Prefer early return on error over deep nesting

```cpp
RetStatus result = DoSomething();
if (STORAGE_FUNC_FAIL(result)) {
    ErrLog(DSTORE_ERROR, MODULE_BUFFER, ErrMsg("DoSomething failed"));
    return result;
}
```

## Memory Management

- Allocate with `DstoreNew(memCtx) ClassName(args)`
- No `std::unique_ptr` / `std::shared_ptr` (project uses custom allocators)
- Always null-check after allocation
- Set pointer to `nullptr` after delete
- Use `DISALLOW_COPY_AND_MOVE(ClassName)` on resource-owning classes

## Class Design

- Use `Initialize()` / `Destroy()` for setup/teardown (not constructor/destructor)
- Constructors should only assign simple defaults
- RAII guards for scoped context switches: `AutoMemCxtSwitch`, `AutoPdbCxtSwitch`
- Inherit from `BaseObject` for traced allocation

## Include Rules

1. Own header first (in .cpp)
2. System headers (`<cstdint>`, `<algorithm>`)
3. Project headers (`"subsystem/dstore_file.h"`)
4. Separate groups with blank line
5. Alphabetical within each group

## Comments

- Prefer `/* */` for descriptions and documentation
- Use `//` only for short inline annotations
- Every file starts with copyright + IDENTIFICATION block
- Document non-obvious logic, not every line

## Best Practices (from similar storage engines)

- Prefer `enum class` over plain `enum`
- Use `constexpr` over `#define` for typed constants
- Use `static_cast<>` over C-style casts
- Initialize member variables at declaration with `{}`
- Prefer `nullptr` over `NULL`
- Mark override methods with `override`
- Use `const` wherever possible (parameters, methods, local vars)
- Avoid global mutable state; use instance members
