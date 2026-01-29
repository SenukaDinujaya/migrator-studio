from ._base import get_operation, list_operations
from ._tracking import (
    OperationRecord,
    SessionTracker,
    get_active_session,
    get_sample_size,
    is_build_mode,
    set_active_session,
)
from .aggregate import (
    groupby_agg,
    groupby_concat,
)
from .apply import (
    apply_column,
    apply_row,
    transform,
)
from .conditional import (
    case,
    coalesce,
    fill_null,
    where,
)
from .convert import (
    to_bool,
    to_int,
    to_numeric,
    to_string,
)
from .date import (
    extract_date_part,
    format_date,
    handle_invalid_dates,
    parse_date,
)
from .dedup import (
    drop_duplicates,
    keep_max,
    keep_min,
)
from .field import (
    concat_columns,
    copy_column,
    drop_columns,
    rename_columns,
    select_columns,
    set_value,
)
from .filter import (
    filter_by_value,
    filter_date,
    filter_isin,
    filter_not_isin,
    filter_not_null,
    filter_null,
    sanitize_data,
)
from .mapping import (
    map_dict,
    map_lookup,
)
from .merge import (
    merge_inner,
    merge_left,
    merge_outer,
)
from .string import (
    str_lower,
    str_regex_replace,
    str_replace,
    str_strip,
    str_upper,
)

__all__ = [
    # Registry
    "list_operations",
    "get_operation",
    # Tracking
    "OperationRecord",
    "SessionTracker",
    "get_active_session",
    "set_active_session",
    "is_build_mode",
    "get_sample_size",
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
