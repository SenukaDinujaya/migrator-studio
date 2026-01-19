# Migrator Studio Specification

A developer tool to build ERP data migration transformers 10x faster.

---

## 1. Problem Statement

### The Pain
Data migration developers currently write transformers by hand. Each transformer requires **200-1000 lines** of repetitive pandas code for common patterns: filtering, mapping, merging, and sometimes building child tables. A single transformer takes **2-8 hours** to develop, with extensive debugging cycles.

### Current State
```python
def transform(sources: dict[str, DataFrame]) -> DataFrame:
    # 200-1000 lines of manual pandas operations
    # Repetitive filtering, merging, mapping patterns
    # Sometimes complex child table building logic
    # Extensive debugging via print statements
    return result_df
```

### Target State
A framework that reduces transformer development to **20-60 minutes** through:
- Configuration-driven transformations (no repetitive coding)
- Live preview during development (see results after each step)
- Pre-built components for common patterns

---

## 2. Product Requirements

### Target Users
Data migration developers building transformers for client ERP migrations.

### Core Capabilities

1. **Configuration-Based Transformation Building**
   - Define transformations declaratively instead of writing code
   - Support for 12 common transformation patterns
   - Escape hatch to custom Python when needed

2. **Live Preview Mode (Marimo)**
   - Step-by-step execution with immediate visual feedback in Marimo notebook
   - During building phase, work with limited sample data (~10 rows)
   - Display results after each transformation step
   - Catch errors early without processing full datasets

3. **Production Execution Mode**
   - Headless execution for full datasets
   - Standard logging (row counts, timing, data types)
   - Performance optimized for millions of rows

4. **Child Table Support** (when needed)
   - Not all transformers require child tables
   - When needed: declarative child table building
   - Conditional child records
   - Aggregation and deduplication in child lists

### Success Criteria (MVP)

The framework is usable when:
- [ ] Developer can build a simple transformer (filter + merge + map) via config
- [ ] Live preview shows 10 sample rows after each step
- [ ] Supports top 5 patterns: Filter, Merge, Map, String ops, Simple child tables
- [ ] At least 1 production transformer migrated to framework
- [ ] 50% time reduction vs manual coding (measured)

### Out of Scope
- Support for data libraries other than pandas
- Automatic migration of existing transformers
- Real-time collaboration features

---

## 3. User Workflow

### Before (Manual Coding)
1. Create new transformer file (TFRM-XXXXXXXX.py)
2. Write `transform(sources)` function manually
3. Code 200-1000 lines of repetitive pandas operations
4. Test by running full transformer on complete dataset
5. Debug by adding print statements and re-running
6. Repeat steps 4-5 many times
7. **Time: 2-8 hours**

### After (Framework-Assisted)
1. Open Marimo notebook with framework
2. Load source tables (only ~10 sample rows during building)
3. Define transformation steps via Python DSL:
   - Add filters, mappings, merges
   - Configure child table rules (when needed)
4. **Live preview in Marimo**: See results after each step
5. Iterate quickly with immediate visual feedback
6. Run on full dataset when ready
7. **Time: 20-60 minutes**

### Example Session (Marimo Notebook)
```python
# Cell 1: Load sources (limited to 10 rows during building)
customers = load_source("DAT-00000001", sample=10)
# Displays: Loaded 10 sample rows from DAT-00000001

# Cell 2: Filter
customers = filter_rows(customers, "Status", in_values=["Active", "Pending"])
# Marimo displays DataFrame preview automatically

# Cell 3: Merge
erpnext_customers = load_source("DAT-00000014", sample=10)
customers = merge_left(customers, erpnext_customers, left_on="CustomerAcct", right_on="legacy_id")
# Marimo displays merged result

# Cell 4: Run on full data when ready
# customers = load_source("DAT-00000001")  # No sample limit
```

---

## 4. Technical Architecture

### High-Level Design
```
┌─────────────────────────────────────────────────────────────┐
│                     Marimo Notebook                          │
│            (Interactive development environment)             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Python DSL                             │
│            (Fluent API for transformation steps)             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Data Operations                          │
│            (Filter, Merge, Map, Child Tables, etc.)          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        pandas                                │
│                  (DataFrame operations)                      │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Responsibility |
|-----------|----------------|
| Marimo Notebook | Interactive UI, live preview, cell-based execution |
| Python DSL | Fluent API functions that wrap pandas operations |
| Data Operations | Pure functions for each transformation type |

### Technology Choices

- **Language**: Python 3.10+
- **Data Library**: pandas
- **Interface**: Marimo (reactive notebook for live preview)
- **Config Format**: Python DSL (fluent API)
- **Testing**: pytest

### Data Flow
```
Source DataFrames (DAT-XXXXXXXX)
        │
        ▼
┌─────────────────┐
│ Load & Sample   │ ← Load ~10 rows during building, full data for production
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ Transform       │ ← Filter, merge, map, string ops, etc.
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ Build Children  │ ← Optional: child tables when needed
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ Finalize        │ ← Select columns, deduplicate, validate
└─────────────────┘
        │
        ▼
Result DataFrame
```

---

## 5. Transformation Patterns

The framework must support these 12 patterns. Each pattern should be:
1. Available as a DSL function (e.g., `filter_rows()`, `merge_left()`)
2. Display results automatically in Marimo after execution
3. Composable with other patterns

### Pattern 1: Filtering & Selection

**Use when**: Selecting subset of rows based on conditions.

```python
# Boolean mask with isin()
df = df[df['Status'].isin(['Active', 'Pending'])]

# Date range filtering
df = df[(df['OrderDate'] >= '2025-01-01') & (df['OrderDate'] <= '2025-12-31')]

# Comparison operators
df = df[df['Amount'] > 100]

# Complex boolean logic
df = df[(df['Status'] == 'Active') & ((df['Region'] == 'North') | (df['Amount'] > 500))]

# Null handling
df = df[df['Email'].notna()]

# Empty string filtering
df = df[df['Name'].fillna('').str.strip() != '']
```

### Pattern 2: Merging & Joining

**Use when**: Combining data from multiple sources.

```python
# LEFT JOIN (keep all from left, match from right)
df = df.merge(lookup_df[['id', 'name']], left_on='lookup_id', right_on='id', how='left')

# INNER JOIN (keep only matches)
df = df.merge(other_df, on=['Key1', 'Key2'], how='inner')

# Multi-column join
df = df.merge(lookup_df, on=['Branch', 'TaxCode'], how='left')
```

### Pattern 3: Mapping & Lookup

**Use when**: Translating values using lookup tables.

```python
# Dictionary mapping
status_map = {'A': 'Active', 'P': 'Pending', 'C': 'Completed'}
df['StatusText'] = df['Status'].map(status_map)

# Mapping with fallback
df['StatusText'] = df['Status'].map(status_map).fillna('Unknown')

# Mapping from DataFrame
lookup_dict = lookup_df.set_index('source_code')['target_name'].to_dict()
df['target_name'] = df['source_code'].map(lookup_dict)
```

### Pattern 4: Field Operations

**Use when**: Creating, copying, or combining columns.

```python
# Simple copy
df['new_col'] = df['source_col']

# Constant value
df['company'] = 'ArrowCorp'

# Concatenation
df['full_name'] = df['first_name'].fillna('') + ' ' + df['last_name'].fillna('')

# Conditional copy
df['primary_contact'] = np.where(df['MailContact'] != '', df['MailContact'], df['ShipContact'])
```

### Pattern 5: Date Handling

**Use when**: Parsing, formatting, or extracting date components.

```python
# Parse dates
df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')

# Format dates
df['OrderDateStr'] = df['OrderDate'].dt.strftime('%Y-%m-%d')

# Handle invalid dates (9999)
df['DueDate'] = df['DueDate'].apply(lambda x: '2099-12-31' if str(x).startswith('9999') else str(pd.to_datetime(x, errors='coerce').date()))

# Extract components
df['year'] = df['OrderDate'].dt.year
df['month'] = df['OrderDate'].dt.month
```

### Pattern 6: Aggregation & Grouping

**Use when**: Summarizing or combining rows.

```python
# Single aggregation
result = df.groupby('Region')['Amount'].sum().reset_index()

# Multiple aggregations
result = df.groupby(['Region', 'Category']).agg({
    'Amount': ['sum', 'mean', 'count'],
    'Quantity': 'sum'
}).reset_index()

# String concatenation in groups
result = df.groupby(['OrderNum', 'Branch'])['Message'].apply(
    lambda msgs: ' '.join(msgs.dropna().astype(str))
).reset_index()

# Collect into lists
result = df.groupby('CustomerId')[['ItemCode', 'Quantity']].apply(
    lambda x: x.values.tolist()
).reset_index()
```

### Pattern 7: Type Conversion

**Use when**: Changing data types.

```python
# To numeric
df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

# To integer
df['Quantity'] = df['Quantity'].fillna(0).astype(int)

# To string
df['Code'] = df['Code'].astype(str)
```

### Pattern 8: Conditional Logic

**Use when**: Applying if-else logic.

```python
# Simple if-else
df['Priority'] = np.where(df['Amount'] > 1000, 'High', 'Normal')

# Nested conditions
df['Priority'] = np.where(df['Amount'] > 1000, 'High',
                 np.where(df['Amount'] > 500, 'Medium', 'Low'))

# Complex logic with apply
df['Category'] = df.apply(
    lambda row: 'VIP' if (row['Amount'] > 1000 and row['Status'] == 'Active') else 'Regular',
    axis=1
)

# Conditional assignment with loc
df.loc[df['Status'] == 'Completed', 'processed'] = 1
df.loc[(df['Amount'] > 500) & (df['Status'] == 'Active'), 'processed'] = 2
```

### Pattern 9: String Operations

**Use when**: Cleaning or transforming text.

```python
# Case conversion
df['name'] = df['name'].str.upper()

# Trim whitespace
df['name'] = df['name'].str.strip()

# Replace
df['phone'] = df['phone'].str.replace('-', '')

# Regex replace (keep only digits)
df['phone'] = df['phone'].str.replace(r'\D', '', regex=True)

# Split
df[['first', 'last']] = df['full_name'].str.split(' ', expand=True)

# Extract name from email
df['name'] = df['email'].str.split('@').str[0].str.replace(r'[^a-zA-Z]', '', regex=True)
```

### Pattern 10: Building Child Tables (Optional)

**Use when**: Creating one-to-many relationships (items, contacts, taxes). Not all transformers need this.

See Section 6 for detailed coverage when needed.

```python
# Simple child table
def create_items(row):
    items = []
    if pd.notna(row['Item1']):
        items.append({'item_code': row['Item1'], 'qty': row['Qty1']})
    if pd.notna(row['Item2']):
        items.append({'item_code': row['Item2'], 'qty': row['Qty2']})
    return items

df['items'] = df.apply(create_items, axis=1)
```

### Pattern 11: Deduplication

**Use when**: Removing duplicate rows or values.

```python
# Single column
df = df.drop_duplicates(subset=['OrderNum'], keep='first')

# Multiple columns
df = df.drop_duplicates(subset=['OrderNum', 'Branch'], keep='last')

# Keep row with highest value in group
df['rank'] = df.groupby(['OrderNum'])['Amount'].rank(method='dense', ascending=False)
df = df[df['rank'] == 1].drop(columns=['rank'])

# Deduplicate list of dicts (using JSON hashing)
def remove_duplicate_dicts(list_of_dicts):
    seen = set()
    unique = []
    for d in list_of_dicts:
        key = json.dumps(d, sort_keys=True, default=str)
        if key not in seen:
            seen.add(key)
            unique.append(d)
    return unique
```

### Pattern 12: Complex/Chained Transformations

**Use when**: Combining multiple patterns in sequence.

```python
def transform(sources):
    df = sources['orders']

    # 1. Filter
    df = df[df['Status'] == 'Active']

    # 2. Parse dates
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    df = df[df['OrderDate'].notna()]

    # 3. Merge
    df = df.merge(lookup_df, on='Region', how='left')

    # 4. Conditional logic
    df['Priority'] = np.where(df['Amount'] > 1000, 'High', 'Normal')

    # 5. String operations
    df['Name'] = df['Name'].str.upper().str.strip()

    # 6. Type conversion
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)

    # 7. Deduplicate
    df = df.drop_duplicates(subset=['OrderNum'], keep='first')

    return df
```

---

## 6. Child Table Patterns (When Needed)

Child tables represent one-to-many relationships stored as lists of dictionaries in DataFrame columns. **Not all transformers need child tables** - many transformers only do filtering, merging, and mapping. When child tables are needed, this is the most complex pattern.

### Structure
```python
# Row with child table
{
    "customer_id": "C001",
    "phone_nos": [
        {"phone": "555-1234", "is_primary_phone": 1},
        {"phone": "555-5678", "is_primary_mobile_no": 1}
    ],
    "email_ids": [
        {"email_id": "john@example.com", "is_primary": 1}
    ]
}
```

### Pattern 6.1: Simple Child Tables

Build child records from multiple columns in the same row.

```python
def create_phone_list(row):
    phones = []
    if row['Phone1']:
        phones.append({'phone': row['Phone1'], 'is_primary': 1})
    if row['Phone2'] and row['Phone2'] != row['Phone1']:
        phones.append({'phone': row['Phone2'], 'is_primary': 0})
    return phones

df['phone_nos'] = df.apply(create_phone_list, axis=1)
```

### Pattern 6.2: Conditional Child Records

Generate child records only when conditions are met.

```python
def create_taxes(row):
    taxes = []
    if row['TaxAmount1'] > 0:
        taxes.append({'account': 'Tax Account 1', 'amount': row['TaxAmount1']})
    if row['TaxAmount2'] > 0:
        taxes.append({'account': 'Tax Account 2', 'amount': row['TaxAmount2']})
    return taxes

df['taxes'] = df.apply(create_taxes, axis=1)
```

### Pattern 6.3: Child Tables from Grouped Data

Build child records by aggregating multiple source rows.

```python
def aggregate_taxes(group):
    tax_dict = defaultdict(float)
    for _, row in group.iterrows():
        tax_dict[row['TaxCode']] += row['TaxAmount']
    return [{'account_head': code, 'amount': amt} for code, amt in tax_dict.items()]

result = []
for (order_num, branch), group in df.groupby(['OrderNum', 'Branch']):
    taxes = aggregate_taxes(group)
    result.append({'OrderNum': order_num, 'Branch': branch, 'taxes': taxes})
result_df = pd.DataFrame(result)
```

### Pattern 6.4: Multi-Step Child Building with Lookups

Complex pattern involving lookups, aggregation, and transformation.

```python
# Step 1: Build items with conversion rate lookup
for (order_num, branch), group in order_line_df.groupby(['OrderNum', 'Branch']):
    items = []
    for _, row in group.iterrows():
        # Get conversion rate from main_df
        conversion_rate = main_df.loc[
            (main_df['OrderNum'] == order_num) & (main_df['Branch'] == branch),
            'conversion_rate'
        ].iloc[0] if not main_df.empty else 1.0

        items.append({
            'item_code': row['ItemCode'],
            'qty': row['Quantity'],
            'rate': row['UnitPrice'],
            'conversion_rate': conversion_rate
        })
    # ... store items
```

### Pattern 6.5: Deduplication in Child Lists

Remove duplicate entries from child table lists.

```python
def deduplicate_child_list(lst):
    seen = set()
    unique = []
    for d in lst:
        key = json.dumps(d, sort_keys=True, default=str)
        if key not in seen:
            seen.add(key)
            unique.append(d)
    return unique

df['items'] = df['items'].apply(deduplicate_child_list)
```

### Pattern 6.6: Consolidate Child Records

Group and sum values within child lists.

```python
def consolidate_taxes(tax_list):
    grouped = defaultdict(float)
    for tax in tax_list:
        key = tax['account_head']
        grouped[key] += tax['amount']
    return [{'account_head': k, 'amount': round(v, 2)} for k, v in grouped.items()]

df['taxes'] = df['taxes'].apply(consolidate_taxes)
```

### Real-World Example: Contact Building

From production transformer TFRM-00000004.py:

```python
def get_mailcontact_phone_child_table(df):
    def create_phone(row):
        phone_data = []

        # Add MailPhone if not empty
        if row['MailPhone'] != '':
            if row['MailPhone'] != row['MailCellPhone']:
                phone_data.append({'phone': row['MailPhone'], 'is_primary_phone': 1, 'is_primary_mobile_no': 0})
            else:
                phone_data.append({'phone': row['MailPhone'], 'is_primary_phone': 1, 'is_primary_mobile_no': 1})

        # Add MailCellPhone if different
        if row['MailCellPhone'] != '' and row['MailCellPhone'] != row['MailPhone']:
            phone_data.append({'phone': row['MailCellPhone'], 'is_primary_phone': 0, 'is_primary_mobile_no': 1})

        # Add TollFreePhone if unique
        if row['TollFreePhone'] != '' and row['TollFreePhone'] not in [row['MailPhone'], row['MailCellPhone']]:
            phone_data.append({'phone': row['TollFreePhone'], 'is_primary_phone': 0, 'is_primary_mobile_no': 0})

        return phone_data

    df['phone_nos'] = df.apply(create_phone, axis=1)
    return df
```

---

## 7. Decisions Made

### Configuration Format: Python DSL
Using a Python DSL (fluent API) because:
- Full power of Python when needed
- No separate config language to learn
- IDE support (autocomplete, type hints)
- Easy to debug

### Live Preview: Marimo
Using Marimo notebooks because:
- Reactive cells - output updates automatically
- Native DataFrame display
- Interactive development experience
- Python-native (no separate tooling)

### Execution Model: Direct Runtime
DSL functions execute directly on DataFrames (no code generation step).
- Simpler architecture
- Easier debugging
- Same code for development and production

---

## 8. Glossary

| Term | Definition |
|------|------------|
| **Transformer** | Python function that transforms source data to target format. Signature: `def transform(sources: dict[str, DataFrame]) -> DataFrame` |
| **Source** | Input DataFrame from legacy system, identified by DAT-XXXXXXXX |
| **Child Table** | One-to-many relationship data stored as list of dicts in a DataFrame column. Example: `phone_nos`, `items`, `taxes` |
| **DAT-XXXXXXXX** | Unique identifier for a source data table (e.g., DAT-00000001 = Customers) |
| **Live Preview** | Interactive development mode in Marimo showing ~10 sample rows after each transformation step |
| **Python DSL** | Fluent API functions for defining transformation steps |
| **ERPNext** | Target ERP system for migrations |

---

## Appendix: Transformer Interface

All transformers follow this contract:

```python
from pandas import DataFrame

def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """
    Transform source data to target format.

    Args:
        sources: Dictionary mapping DAT-XXXXXXXX identifiers to DataFrames.
                 Example: {"DAT-00000001": customer_df, "DAT-00000004": vendor_df}

    Returns:
        DataFrame: Transformed data ready for import to target system.
                   May contain child table columns (lists of dicts).
    """
    # Implementation here
    return result_df
```
