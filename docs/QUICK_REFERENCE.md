# Transformation Patterns - Quick Reference

**Quick lookup for all 12 transformation pattern categories**

---

## 1. Filtering & Selection

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| Boolean mask | `df[df['Status'].isin(['Active'])]` | Filter by values |
| Date range | `df[(df['Date'] >= start) & (df['Date'] <= end)]` | Date filtering |
| Comparison | `df[df['Amount'] > 100]` | Numeric filtering |
| Complex logic | `df[(df['A'] > 0) & (df['B'] == 'X') | (df['C'] != 0)]` | Multiple conditions |
| Non-null | `df[df['Field'].notna()]` | Remove nulls |
| Empty strings | `df[df['Field'].fillna('').str.strip() != '']` | Remove empty |

**Config Example**:
```yaml
- type: filter
  conditions:
    - field: Status
      operator: in
      values: [Active, Pending]
    - field: Amount
      operator: ">"
      value: 100
```

---

## 2. Merging & Joining

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| Left join | `df.merge(lookup, on='key', how='left')` | Keep all left rows |
| Inner join | `df.merge(other, on='key', how='inner')` | Keep only matches |
| Multi-key | `df.merge(lookup, on=['key1', 'key2'], how='left')` | Composite keys |

**Config Example**:
```yaml
- type: merge
  source: lookup_table
  left_on: CustomerID
  right_on: ID
  how: left
  select_columns: [Name, Region]
```

---

## 3. Mapping & Lookups

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| Dictionary map | `df['NewCol'] = df['OldCol'].map({'A': 'Alpha', 'B': 'Beta'})` | Value translation |
| With fallback | `df['Col'].map(mapping).fillna('Unknown')` | Handle unmapped |
| From DataFrame | `lookup_dict = lookup.set_index('code')['name'].to_dict()` | DataFrame lookup |

**Config Example**:
```yaml
- type: map
  field: StatusCode
  output: StatusText
  mapping:
    A: Active
    P: Pending
    C: Completed
  default: Unknown
```

---

## 4. Field Operations

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| Copy field | `df['NewCol'] = df['OldCol']` | Duplicate column |
| Set constant | `df['Type'] = 'Customer'` | Fixed value |
| Concatenate | `df['Full'] = df['First'] + ' ' + df['Last']` | Combine strings |
| Conditional | `df['Result'] = np.where(condition, val1, val2)` | If-then-else |

**Config Example**:
```yaml
- type: field_operations
  operations:
    - copy: {from: OldName, to: NewName}
    - set: {field: Type, value: "Customer"}
    - concat: {fields: [FirstName, LastName], separator: " ", output: FullName}
```

---

## 5. Date Handling

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| Parse dates | `df['Date'] = pd.to_datetime(df['Date'], errors='coerce')` | String → datetime |
| Format dates | `df['DateStr'] = df['Date'].dt.strftime('%Y-%m-%d')` | Datetime → string |
| Invalid dates | `df['Date'].apply(lambda x: '2099-12-31' if str(x).startswith('9999') else x)` | Handle 9999 dates |
| Extract parts | `df['Year'] = df['Date'].dt.year` | Get year/month/day |

**Config Example**:
```yaml
- type: date_operations
  operations:
    - parse: {field: OrderDate, errors: coerce}
    - format: {field: OrderDate, output: OrderDateStr, format: "%Y-%m-%d"}
    - extract: {field: OrderDate, components: [year, month]}
```

---

## 6. Aggregation & Grouping

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| Simple agg | `df.groupby('Region')['Amount'].sum()` | Sum by group |
| Multi-function | `df.groupby('Region').agg({'Amount': ['sum', 'mean']})` | Multiple aggregations |
| String concat | `df.groupby('ID')['Message'].apply(lambda x: ' '.join(x))` | Combine strings |
| Collect lists | `df.groupby('ID')['Item'].apply(list)` | Group into lists |

**Config Example**:
```yaml
- type: aggregate
  group_by: [Region, Category]
  aggregations:
    Amount: [sum, mean, count]
    Quantity: max
    Message: join  # String concatenation
```

---

## 7. Type Conversion

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| To numeric | `df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')` | String → number |
| To integer | `df['Qty'] = df['Qty'].fillna(0).astype(int)` | Float → int |
| To string | `df['Code'] = df['Code'].astype(str)` | Any → string |
| To boolean | `df['Flag'] = (df['Value'] != 0).astype(bool)` | Int → bool |

**Config Example**:
```yaml
- type: convert_types
  conversions:
    Amount: numeric
    Quantity: integer
    Code: string
    IsActive: boolean
```

---

## 8. Conditional Logic

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| Simple if-else | `df['Result'] = np.where(df['A'] > 10, 'High', 'Low')` | Binary condition |
| Nested | `np.where(df['A'] > 100, 'VIP', np.where(df['A'] > 50, 'Premium', 'Regular'))` | Multiple levels |
| Complex logic | `df['Cat'] = df.apply(lambda row: 'X' if complex_logic(row) else 'Y', axis=1)` | Row-wise logic |
| Loc assignment | `df.loc[df['Status'] == 'Active', 'Flag'] = 1` | Conditional update |

**Config Example**:
```yaml
- type: conditional
  field: Category
  logic:
    - condition: "Amount > 1000"
      value: VIP
    - condition: "Amount > 500"
      value: Premium
    - default: Regular
```

---

## 9. String Operations

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| Upper/lower | `df['Name'] = df['Name'].str.upper()` | Case conversion |
| Strip | `df['Text'] = df['Text'].str.strip()` | Remove whitespace |
| Replace | `df['Phone'] = df['Phone'].str.replace(r'\D', '', regex=True)` | Clean phone numbers |
| Split | `df[['First', 'Last']] = df['Name'].str.split(' ', expand=True)` | Split strings |
| Extract | `df['Username'] = df['Email'].str.split('@').str[0]` | Parse email |

**Config Example**:
```yaml
- type: string_operations
  operations:
    - field: Name
      operation: upper
    - field: Phone
      operation: regex_replace
      pattern: '\D'
      replacement: ''
      output: CleanPhone
```

---

## 10. Building Child Tables (Simple)

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| Simple list | `df['items'] = df.apply(lambda r: [{'code': r['Item1']}] if pd.notna(r['Item1']) else [], axis=1)` | Basic child records |
| Conditional | `df['items'] = df.apply(create_items_func, axis=1)` | Conditional children |

**Config Example**:
```yaml
- type: build_child_table
  output: phone_nos
  records:
    - condition: "row['MailPhone'] != ''"
      fields:
        phone: row['MailPhone']
        is_primary: 1
    - condition: "row['CellPhone'] != '' and row['CellPhone'] != row['MailPhone']"
      fields:
        phone: row['CellPhone']
        is_primary: 0
```

---

## 11. Advanced Child Tables

**Complex patterns** - Usually require custom Python functions:

- Multi-step grouping and aggregation
- Tax consolidation with defaultdict
- Deduplication using JSON hashing
- Message concatenation with ordering
- Sales team allocation with percentages

**See**: `TFRM-COMPREHENSIVE-DEMO.py:572` for complete example

**Config Strategy**: Allow custom function reference
```yaml
- type: custom
  function: "custom_logic.build_advanced_child_tables"
  params:
    tax_template_source: DAT-00000123
```

---

## 12. Deduplication

| Pattern | Pandas Code | Use Case |
|---------|-------------|----------|
| Single column | `df.drop_duplicates(subset=['ID'], keep='first')` | Unique by one field |
| Multiple columns | `df.drop_duplicates(subset=['A', 'B'], keep='last')` | Composite unique |
| Keep highest | `df.groupby('ID')['Amount'].max()` then filter | Best record per group |
| Dict list | Use JSON hashing (see TFRM-COMPREHENSIVE-DEMO.py:720) | Dedupe child records |

**Config Example**:
```yaml
- type: deduplicate
  subset: [CustomerID, Date]
  keep: first
```

---

## Priority Order for Implementation

1. **Filter** (Most common, easiest)
2. **Select columns** (Simple, frequent)
3. **Merge** (Critical for lookups)
4. **String operations** (High frequency)
5. **Simple child tables** (Core ERP pattern)
6. Map/Lookup
7. Field operations
8. Type conversion
9. Conditional logic
10. Date handling
11. Aggregation
12. Deduplication
13. Advanced child tables

---

## Reference Code Locations

All patterns demonstrated in:
- **`TFRM-COMPREHENSIVE-DEMO.py`** - Lines 30-933 (functions for each pattern)
- **`TFRM-00000004.py`** - Real production usage (contacts transformer)

---

**For detailed examples, see `project_document.md` section "Required Data Manipulation Capabilities"**
