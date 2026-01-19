"""
migrator_studio - Build ERP data migration transformers faster.

Example:
    with BuildSession(sample=10) as session:
        df = load_source("DAT-00000001")
        df = filter_isin(df, "Status", ["Active"])
        df = merge_left(df, regions, on="RegionID")
        print(session.summary())
"""

__version__ = "0.1.0"

from .config import configure, get_config
from .session import BuildSession
from .loader import load_source
from .step import step

# Registry functions
from .ops._base import list_operations, get_operation

# Filter (Pattern 1)
from .ops.filter import (
    filter_isin,
    filter_not_isin,
    filter_by_value,
    filter_null,
    filter_not_null,
    filter_date,
    sanitize_data,
)

# Merge (Pattern 2)
from .ops.merge import (
    merge_left,
    merge_inner,
    merge_outer,
)

# Mapping (Pattern 3)
from .ops.mapping import (
    map_dict,
    map_lookup,
)

# Field (Pattern 4)
from .ops.field import (
    copy_column,
    set_value,
    concat_columns,
    rename_columns,
    drop_columns,
    select_columns,
)

# Date (Pattern 5)
from .ops.date import (
    parse_date,
    format_date,
    extract_date_part,
    handle_invalid_dates,
)

# Aggregate (Pattern 6)
from .ops.aggregate import (
    groupby_agg,
    groupby_concat,
)

# Convert (Pattern 7)
from .ops.convert import (
    to_numeric,
    to_int,
    to_string,
    to_bool,
)

# Conditional (Pattern 8)
from .ops.conditional import (
    where,
    case,
    fill_null,
    coalesce,
)

# String (Pattern 9)
from .ops.string import (
    str_upper,
    str_lower,
    str_strip,
    str_replace,
    str_regex_replace,
)

# Dedup (Pattern 11)
from .ops.dedup import (
    drop_duplicates,
    keep_max,
    keep_min,
)

# Apply (Pattern 12)
from .ops.apply import (
    apply_row,
    apply_column,
    transform,
)

__all__ = [
    "__version__",
    "configure",
    "get_config",
    "BuildSession",
    "load_source",
    "step",
    # Registry
    "list_operations",
    "get_operation",
    # Filter (Pattern 1)
    "filter_isin",
    "filter_not_isin",
    "filter_by_value",
    "filter_null",
    "filter_not_null",
    "filter_date",
    "sanitize_data",
    # Merge (Pattern 2)
    "merge_left",
    "merge_inner",
    "merge_outer",
    # Mapping (Pattern 3)
    "map_dict",
    "map_lookup",
    # Field (Pattern 4)
    "copy_column",
    "set_value",
    "concat_columns",
    "rename_columns",
    "drop_columns",
    "select_columns",
    # Date (Pattern 5)
    "parse_date",
    "format_date",
    "extract_date_part",
    "handle_invalid_dates",
    # Aggregate (Pattern 6)
    "groupby_agg",
    "groupby_concat",
    # Convert (Pattern 7)
    "to_numeric",
    "to_int",
    "to_string",
    "to_bool",
    # Conditional (Pattern 8)
    "where",
    "case",
    "fill_null",
    "coalesce",
    # String (Pattern 9)
    "str_upper",
    "str_lower",
    "str_strip",
    "str_replace",
    "str_regex_replace",
    # Dedup (Pattern 11)
    "drop_duplicates",
    "keep_max",
    "keep_min",
    # Apply (Pattern 12)
    "apply_row",
    "apply_column",
    "transform",
]
