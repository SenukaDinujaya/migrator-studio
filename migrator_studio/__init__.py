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

# Display
from .display import diff, preview, summary
from .loader import load_source

# Registry functions
from .operations._base import get_operation, list_operations

# Aggregate (Pattern 6)
from .operations.aggregate import (
    groupby_agg,
    groupby_concat,
)

# Apply (Pattern 12)
from .operations.apply import (
    apply_column,
    apply_row,
    transform,
)

# Conditional (Pattern 8)
from .operations.conditional import (
    case,
    coalesce,
    fill_null,
    where,
)

# Convert (Pattern 7)
from .operations.convert import (
    to_bool,
    to_int,
    to_numeric,
    to_string,
)

# Date (Pattern 5)
from .operations.date import (
    extract_date_part,
    format_date,
    handle_invalid_dates,
    parse_date,
)

# Dedup (Pattern 11)
from .operations.dedup import (
    drop_duplicates,
    keep_max,
    keep_min,
)

# Field (Pattern 4)
from .operations.field import (
    concat_columns,
    copy_column,
    drop_columns,
    rename_columns,
    select_columns,
    set_value,
)

# Filter (Pattern 1)
from .operations.filter import (
    filter_by_value,
    filter_date,
    filter_isin,
    filter_not_isin,
    filter_not_null,
    filter_null,
    sanitize_data,
)

# Mapping (Pattern 3)
from .operations.mapping import (
    map_dict,
    map_lookup,
)

# Merge (Pattern 2)
from .operations.merge import (
    merge_inner,
    merge_left,
    merge_outer,
)

# String (Pattern 9)
from .operations.string import (
    str_lower,
    str_regex_replace,
    str_replace,
    str_strip,
    str_upper,
)
from .session import BuildSession
from .step import step

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
    # Display
    "preview",
    "summary",
    "diff",
]
