# Transformer Framework

**A developer tool to build ERP data migration transformers 10x faster**

---

## What is This?

A configuration-driven framework for building data transformation scripts (transformers) used in ERP migrations. Instead of manually coding 50-200 lines of repetitive pandas operations, developers define transformations via config and get:

- **Live preview**: See results after each transformation step (10 sample rows)
- **Auto-generated code**: Config → Production-ready Python transformer
- **Built-in patterns**: Pre-built components for 12 common transformation types
- **Rapid iteration**: Build transformers in 20-60 minutes instead of 2-8 hours

---

## Quick Start

### Current Status: **Requirements Phase**

This project is in the design phase. See `project_document.md` for complete requirements.

### Project Structure

```
transformer_framework/
├── README.md                          # This file
├── project_document.md                # Complete requirements specification
├── docs/
│   └── examples/
│       ├── TFRM-COMPREHENSIVE-DEMO.py # Reference: All 12 transformation patterns
│       └── TFRM-00000004.py           # Real production transformer example
└── archive/                           # Historical/reference documents
```

---

## Documentation

### Essential Reading

1. **`project_document.md`** - Start here! Complete requirements, examples, design decisions
2. **`docs/examples/TFRM-COMPREHENSIVE-DEMO.py`** - Reference implementation of all patterns
3. **`docs/examples/TFRM-00000004.py`** - Real-world production example

### Archive (Background Context)

The `archive/` directory contains historical documents:
- `jira_task.md` - Original Jira ticket (MAIN-377)
- `PROPOSAL_Transformer_Framework.md` - Initial proposal
- `PROJECT_CONTEXT.md` - Background on the migrator platform
- Other reference materials

---

## Development Roadmap

### Phase 1: Research & POC (Week 1)
- [ ] Document requirements ✅
- [ ] Identify top 10 most common patterns
- [ ] Create POC: YAML config for filter + merge + map
- [ ] Build basic config parser
- [ ] Test with TFRM-00000004.py subset

### Phase 2: Core Development (Week 2)
- [ ] Implement top 5 transformation types
- [ ] Build live preview system (CLI)
- [ ] Create code generator (config → Python)
- [ ] Add error handling
- [ ] Documentation + examples

### Phase 3: Validation
- [ ] Transformer build testing
- [ ] Live data feedback testing
- [ ] Fix any bugs or making necessary adjustments.

---

## Key Concepts

### Transformer
Python script that transforms source data (legacy ERP) to target format (ERPNext):

```python
def transform(sources: dict[str, DataFrame]) -> DataFrame:
    # Manual coding of filters, merges, mappings, child tables, etc.
    return result_df
```

### Child Table
One-to-many relationship data stored as list of dicts in DataFrame column:

```python
# Parent row with child records
{
    "customer_id": "C001",
    "phone_nos": [
        {"phone": "555-1234", "is_primary": 1},
        {"phone": "555-5678", "is_primary": 0}
    ]
}
```

### Live Preview
Interactive development mode showing sample data after each transformation step.

---

## Goal

**Reduce transformer development time from 2-8 hours to 20-60 minutes** while maintaining code quality and flexibility.

---

## Technology Stack (Planned)

- **Language**: Python 3.10+
- **Data library**: pandas (with future Polars consideration)
- **Config format**: YAML + Python DSL (hybrid approach)
- **Display**: CLI with `rich` for formatting
- **Testing**: pytest

---

## Contributing

This is an internal SGA Tech Solutions project. Development team members should:

1. Read `project_document.md` completely
2. Review example transformers in `docs/examples/`
3. Provide feedback on design decisions (see project_document.md sections)

---

## Contact

**Project Lead**: Senuka Dinujaya Withana Arachchige
**Jira Ticket**: MAIN-377
**Status**: Requirements & Design Phase
