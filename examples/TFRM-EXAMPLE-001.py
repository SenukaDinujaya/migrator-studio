"""
TFRM-EXAMPLE-001: Customer Transformer

Transforms customer data from legacy ERP format to target format.
"""
from pandas import DataFrame

from migrator_studio import (
    step,
    filter_isin,
    filter_not_null,
    sanitize_data,
    merge_left,
    map_dict,
    set_value,
    copy_column,
    rename_columns,
    select_columns,
    str_strip,
    str_upper,
    where,
    fill_null,
    drop_duplicates,
)

SOURCES = ["DAT-00000001", "DAT-00000005", "DAT-00000006"]


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Transform customer data to target format."""
    customers = sources["DAT-00000001"]
    regions = sources["DAT-00000005"]
    statuses = sources["DAT-00000006"]

    step("Sanitize data")
    df = sanitize_data(customers, empty_val=None)

    step("Filter active customers")
    df = filter_isin(df, "Status", ["A", "P"])

    step("Filter customers with names")
    df = filter_not_null(df, "Name")

    step("Merge with region lookup")
    df = merge_left(
        df,
        regions,
        left_on="Region",
        right_on="RegionCode",
        select_columns=["RegionName"],
    )

    step("Map status codes")
    status_map = statuses.set_index("StatusCode")["StatusDesc"].to_dict()
    df = map_dict(df, "Status", status_map, target="StatusText")

    step("Clean string fields")
    df = str_strip(df, "Name")
    df = str_upper(df, "Name")

    step("Handle null values")
    df = fill_null(df, "Email", "no-email@placeholder.com")
    df = fill_null(df, "Phone", "N/A")
    df = fill_null(df, "RegionName", "Unknown Region")

    step("Set customer fields")
    df = copy_column(df, "Name", "customer_name")
    df = copy_column(df, "CustomerID", "legacy_id")

    step("Set constant fields")
    df = set_value(df, "customer_type", "Company")
    df = set_value(df, "customer_group", "Retail")
    df = set_value(df, "company", "Default Company")

    step("Create priority flag")
    df = where(
        df,
        column="priority",
        condition=df["Region"].isin(["NA", "EU"]),
        then_value="High",
        else_value="Normal",
    )

    step("Deduplicate")
    df = drop_duplicates(df, columns="CustomerID", keep="first")

    step("Select final columns")
    df = select_columns(
        df,
        [
            "customer_name",
            "legacy_id",
            "customer_type",
            "customer_group",
            "StatusText",
            "RegionName",
            "Email",
            "Phone",
            "Address",
            "company",
            "priority",
        ],
    )
    df = rename_columns(
        df,
        {
            "StatusText": "status",
            "RegionName": "region",
            "Email": "email",
            "Phone": "phone",
            "Address": "address",
        },
    )

    return df
