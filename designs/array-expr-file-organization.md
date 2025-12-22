# Array Expression File Organization

## Motivation
Large files (500+ lines) are difficult for AI agents to work with effectively. Splitting into focused files improves:
- Agent comprehension (one concept per file)
- Targeted modifications without full-file context
- Parallel development across features

## Principles

1. **One concept per file**: Each file contains one class/function and its direct helpers
2. **Target size**: 50-200 lines per file. Larger is OK if tightly coupled.
3. **Subdirectory structure**: Group related files in subdirectories (e.g., `io/`, `linalg/`, `creation/`)
4. **Re-exports for compatibility**: Original file (e.g., `_io.py`) becomes pure re-exports
5. **Clear `__init__.py`**: Subdirectory `__init__.py` exports all public APIs

## File Naming
- `_concept.py` for implementation files (underscore prefix)
- Expression class + its public function live together (e.g., `FromDelayed` class + `from_delayed` function)
- Helper functions stay with their primary user

## What to Split
- Files with multiple unrelated public functions/classes
- Files over 500 lines with natural boundaries

## What NOT to Split
- Tightly coupled code (e.g., `_collection.py` Array class)
- Single-concept files already at good size
- Base classes used by many files (keep in `_expr.py`, `_base.py`)

## Pattern
```
module/
  __init__.py      # re-exports all public APIs
  _concept_a.py    # ConceptA class + concept_a function
  _concept_b.py    # ConceptB class + concept_b function
  _utils.py        # shared helpers (if needed)

_module.py         # pure re-exports for backward compatibility
```
