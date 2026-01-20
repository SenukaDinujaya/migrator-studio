from ._base import list_operations, get_operation
from ._tracking import (
    OperationRecord,
    SessionTracker,
    get_active_session,
    set_active_session,
    is_build_mode,
    get_sample_size,
)

from .filter import (
    filter_isin,
    filter_not_isin,
    filter_by_value,
    filter_null,
    filter_not_null,
    filter_date,
    sanitize_data,
)
from .merge import (
    merge_left,
    merge_inner,
    merge_outer,
)
from .mapping import (
    map_dict,
    map_lookup,
)
from .string import (
    str_upper,
    str_lower,
    str_strip,
    str_replace,
    str_regex_replace,
)
from .field import (
    copy_column,
    set_value,
    concat_columns,
    rename_columns,
    drop_columns,
    select_columns,
)
from .convert import (
    to_numeric,
    to_int,
    to_string,
    to_bool,
)
from .conditional import (
    where,
    case,
    fill_null,
    coalesce,
)
from .dedup import (
    drop_duplicates,
    keep_max,
    keep_min,
)
from .date import (
    parse_date,
    format_date,
    extract_date_part,
    handle_invalid_dates,
)
from .aggregate import (
    groupby_agg,
    groupby_concat,
)
from .apply import (
    apply_row,
    apply_column,
    transform,
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
