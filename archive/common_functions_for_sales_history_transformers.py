"""
Common Functions for Sales History Transformers

This module contains shared functionality for sales orders, delivery notes, and sales invoices
transformers. It implements parameterized functions to handle variations between different
document types while maintaining consistency in core business logic.

Author: Senuka Dinujaya Withana Arachchige -SGA Tech Solutions
Date: 2025
"""

import numpy as np
import pandas as pd
from pandas import DataFrame
from collections import defaultdict
from typing import Dict, List, Any, Tuple, Optional, Union
from logging import Logger
import json
from decimal import Decimal
from  datetime import datetime
from sga_migrator.sga_migrator.doctype.transformer.transformers.data_cleaning_utils import (
    clean_data, 
    get_logger
)


def filter_sales_orders(df: DataFrame, trans_type_filter: str = 'S') -> DataFrame:
    """
    Filter records by transaction type.
    
    Args:
        df: Input DataFrame
        trans_type_filter: Transaction type to filter by (default: 'S')
        
    Returns:
        DataFrame: Filtered dataframe containing only specified transaction type
    """
    return df[df["TransType"] == trans_type_filter].reset_index(drop=True)


def filter_open_orders(df: DataFrame, document_type: str) -> DataFrame:
    """
    Filter for open orders based on document type.

    Args:
        df: Input DataFrame 
        document_type: Type of document ('sales_orders', 'delivery_notes', 'sales_invoices')

    Returns:
        DataFrame: Filtered dataframe containing only open orders
    """
    closed_statuses = ['C', 'P', 'J', 'Completed', 'Closed', 'Cancelled']

    if document_type == 'sales_orders':
        open_status_list = ['I','O','S']
        filtered = df[df['Status'].isin(open_status_list)]
        filtered = filtered[~filtered['Status'].isin(closed_statuses)]
        return filtered.reset_index(drop=True)

    elif document_type == 'delivery_notes':
        open_status_list = ['D', 'S','I']
        shipping_not_completed_status_list = ['S', 'D','I']
        mask = (
            (df['Status'].isin(open_status_list)) |
            (
                df['Status'].isin(['I']) &
                df['ShipStatus'].isin(shipping_not_completed_status_list) &
                (df['TotalAmt'] > df['TotalPaid'])
            )
        )
        filtered = df[mask]
        filtered = filtered[~filtered['Status'].isin(closed_statuses)]
        return filtered.reset_index(drop=True)

    elif document_type == 'sales_invoices':
        open_status_list = ['I']
        shipping_not_completed_status_list = [ 'S', 'D']
        filtered = df[
            df['Status'].isin(open_status_list) &
            df['ShipStatus'].isin(shipping_not_completed_status_list) &
            (df['TotalAmt'] > df['TotalPaid'])
        ]
        filtered = filtered[~filtered['Status'].isin(closed_statuses)]
        return filtered.reset_index(drop=True)
    return df[~df['Status'].isin(closed_statuses)].reset_index(drop=True)


def filter_by_branch(df: DataFrame, branches: List[str] = None) -> DataFrame:
    """
    Filter orders by specific branches.
    
    Args:
        df: Input DataFrame
        branches: List of branch codes to keep (default: ['01', 'PG'])
        
    Returns:
        DataFrame: Filtered dataframe containing only specified branches
    """
    if branches is None:
        branches = ['PG']
    
    return df[df["Branch"].isin(branches)].reset_index(drop=True)


def apply_cutoff_date(df: DataFrame, cutoff_date: str = "2018-01-01", 
                     date_column: str = 'dtCreated') -> DataFrame:
    """
    Filter records to include only those after a specified cutoff date.
    
    Args:
        df: Input DataFrame
        cutoff_date: ISO date string for cutoff (default: '2018-01-01')
        date_column: Column name containing the date (default: 'dtCreated')
        
    Returns:
        DataFrame: Filtered dataframe with dates after cutoff
    """
    cutoff = pd.to_datetime(cutoff_date)
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    return df[df[date_column] > cutoff].reset_index(drop=True)


def merge_customer_data(df: DataFrame, customers_df: DataFrame, 
                       merge_column: str = 'BillCust') -> DataFrame:
    """
    Merge customer information with main dataframe.
    
    Args:
        df: Main DataFrame
        customers_df: Customer DataFrame with legacy_id and name columns
        merge_column: Column to merge on (default: 'BillCust')
        
    Returns:
        DataFrame: Merged dataframe with customer information
    """
    customers = customers_df.rename(
        columns={'legacy_id': merge_column, 'name': 'customer'}
    )
    
    merged_df = df.merge(
        customers[[merge_column, 'customer']], 
        on=merge_column, 
        how='left'
    )
    
    return merged_df


def merge_fob_data(df: DataFrame, fob_df: DataFrame) -> DataFrame:
    """
    Merge FOB (Free On Board) information with main dataframe.
    
    Args:
        df: Main DataFrame
        fob_df: FOB DataFrame
        
    Returns:
        DataFrame: Merged dataframe with FOB information
    """
    fob_df = fob_df.rename(
        columns={'QUANTUS FOB': 'ShipFob', 'ERPNext FOB': 'fob'}
    )

    merged_df = df.merge(
        fob_df[['Branch', 'ShipFob', 'fob']], 
        on=['Branch', 'ShipFob'], 
        how='left'
    )

    return merged_df


def map_delivery_method(df: DataFrame, tailer_codes_df: DataFrame) -> DataFrame:
    """
    Map carrier codes to delivery method descriptions.
    
    Args:
        df: Main DataFrame
        tailer_codes_df: DataFrame containing carrier code mappings
        
    Returns:
        DataFrame: DataFrame with mapped delivery method descriptions
    """
    # Filter for carrier type codes only
    tailer_codes = tailer_codes_df[tailer_codes_df['Type'] == 'C'].copy()
    tailer_codes.loc[tailer_codes['Code'] == 'CRAP', 'Description'] = 'CRAP'

    df['deliver_via'] = df['Carrier'].map(
        tailer_codes.set_index('Code')['Description']
    ).str.upper()

    return df


def set_customer_field(df: DataFrame, source_column: str = 'name') -> DataFrame:
    """
    Set customer field from source column.
    
    Args:
        df: Input DataFrame
        source_column: Column to copy customer data from (default: 'name')
        
    Returns:
        DataFrame: DataFrame with customer field populated
    """
    df['customer'] = df[source_column]
    return df


def set_order_type(df: DataFrame, order_type_value: str = 'Sales') -> DataFrame:
    """
    Set order type for all records.
    
    Args:
        df: Input DataFrame
        order_type_value: Value to set for order_type (default: 'Sales')
        
    Returns:
        DataFrame: DataFrame with order_type field populated
    """
    df['order_type'] = order_type_value
    return df


def map_company_by_branch(df: DataFrame) -> DataFrame:
    """
    Map branch codes to company names.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with company field populated
    """
    df['company'] = np.where(
        df['Branch'] == '01',
        'ArrowCorp',
        'Premier Grain Cleaner'
    )
    return df


def set_skip_delivery_note(df: DataFrame, skip_value: int) -> DataFrame:
    """
    Set skip_delivery_note flag.
    
    Args:
        df: Input DataFrame
        skip_value: Value to set (0 for sales_orders/delivery_notes, 1 for sales_invoices)
        
    Returns:
        DataFrame: DataFrame with skip_delivery_note field populated
    """
    df['skip_delivery_note'] = skip_value
    return df


def format_date_field(df: DataFrame, source_column: str, target_column: str) -> DataFrame:
    """
    Convert and format date fields, handling invalid dates.
    
    Args:
        df: Input DataFrame
        source_column: Source date column name
        target_column: Target date column name
        
    Returns:
        DataFrame: DataFrame with formatted date field
    """
    df[target_column] = df[source_column]
    df[target_column] = df[target_column].apply(
        lambda x: '2099-12-31' if str(x).startswith('9999') 
        else str(pd.to_datetime(x, errors='coerce').date())
    )
    df[target_column] = pd.to_datetime(df[target_column]).dt.date.astype(str)
    return df


def copy_field(df: DataFrame, source_column: str, target_column: str) -> DataFrame:
    """
    Copy data from source column to target column.
    
    Args:
        df: Input DataFrame
        source_column: Source column name
        target_column: Target column name
        
    Returns:
        DataFrame: DataFrame with target field populated
    """
    df[target_column] = df[source_column]
    return df


def map_cost_center_by_branch(df: DataFrame) -> DataFrame:
    """
    Map branch codes to cost center names.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with cost_center field populated
    """
    df['cost_center'] = df['Branch'].apply(
        lambda x: '01 - ArrowCorp - AC' if x == '01' else '01 - Premier - PG'
    )
    return df


def set_drop_ship_order_flag(df: DataFrame) -> DataFrame:
    """
    Flag orders with 'DROP' ShipCust as drop ship orders.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with drop_ship_order flag populated
    """
    df['drop_ship_order'] = df['ShipCust'] == 'DROP'
    return df


def construct_drop_ship_address(df: DataFrame, sec_addresses_df: DataFrame) -> DataFrame:
    """
    Construct drop ship addresses for orders with 'DROP' ShipCust.
    
    Args:
        df: Main DataFrame
        sec_addresses_df: DataFrame containing secondary address information
        
    Returns:
        DataFrame: DataFrame with drop_ship_address field populated
    """
    # drop_mask = df['ShipCust'] == 'DROP'
    df['drop_ship_order'] = True
    drop_mask = df['drop_ship_order'] == True
    sec_addresses_df['OrderNum'] = sec_addresses_df['OrderNum'].astype(str)

    sec_addresses_df['drop_ship_address'] = (
        sec_addresses_df['Name1'].fillna('') + '\n' +
        sec_addresses_df['Name2'].fillna('') + '\n' +
        sec_addresses_df['Address1'].fillna('') + '\n' +
        sec_addresses_df['Address2'].fillna('') + '\n' +
        sec_addresses_df['City'].fillna('') + ', ' + sec_addresses_df['Prov'].fillna('') + '\n' +
        sec_addresses_df['Country'].fillna('')
    ).str.replace(r'\n+', '\n', regex=True).str.strip()

    arddress_df = sec_addresses_df[['OrderNum','Branch','drop_ship_address']]
    df = df.merge(arddress_df, how='left', on=['OrderNum','Branch'])
    
    return df

def determine_currency_info(df: DataFrame) -> DataFrame:
    """
    Determine currency and price list based on forex and branch information.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with currency-related fields populated
    """
    forex = df['ForExch'].fillna('').astype(str).str.strip().str.upper()
    branch = df['Branch'].fillna('').astype(str).str.strip()

    currency = []
    price_list_currency = []
    selling_price_list = []

    for fx, br in zip(forex, branch):
        if fx in ['US', 'USD']:
            currency.append('USD')
            price_list_currency.append('USD')
            selling_price_list.append('Standard Selling USD')
        elif fx in ['CDN', 'RCDN', 'CAD']:
            currency.append('CAD')
            price_list_currency.append('CAD')
            selling_price_list.append('Standard Selling CAD')
        elif fx == '':
            if br == '01':
                currency.append('CAD')
                price_list_currency.append('CAD')
                selling_price_list.append('Standard Selling CAD')
            elif br == 'PG':
                currency.append('USD')
                price_list_currency.append('USD')
                selling_price_list.append('Standard Selling USD')
            else:
                currency.append('')
                price_list_currency.append('')
                selling_price_list.append('')
        else:
            currency.append(fx)
            price_list_currency.append(fx)
            selling_price_list.append('')

    df['currency'] = currency
    df['price_list_currency'] = price_list_currency
    df['selling_price_list'] = selling_price_list
    df['plc_conversion_rate'] = 1.0

    return df


def calculate_conversion_rate(df: DataFrame) -> DataFrame:
    """
    Set conversion rate based on forex and exchange rate information.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with conversion_rate field populated
    """
    condition = (
        df['ForExch'].isna() |
        (df['ForExch'].astype(str).str.strip() == '') |
        (df['ExchRate'].astype(float) == 0.0)
    )
    
    df['conversion_rate'] = np.where(
        condition,
        1,
        df['ExchRate']
    )
    
    return df


def set_discount_amount(df: DataFrame, source_column: str = 'DiscAmt') -> DataFrame:
    """
    Set discount amount from source column.
    
    Args:
        df: Input DataFrame
        source_column: Source column for discount amount (default: 'DiscAmt')
        
    Returns:
        DataFrame: DataFrame with discount_amount field populated
    """
    df['discount_amount'] = df[source_column].fillna(0.0)
    return df


def set_base_grand_total(df: DataFrame, source_column: str = 'TotalAmt') -> DataFrame:
    """
    Set base grand total from source column.
    
    Args:
        df: Input DataFrame
        source_column: Source column for total amount (default: 'TotalAmt')
        
    Returns:
        DataFrame: DataFrame with base_grand_total field populated
    """
    df['base_grand_total'] = df[source_column].fillna(0.0)
    return df


def set_rounding_adjustment(df: DataFrame, value: Union[int, float] = 0) -> DataFrame:
    """
    Set rounding adjustment value.
    
    Args:
        df: Input DataFrame
        value: Value to set for rounding_adjustment (default: 0)
        
    Returns:
        DataFrame: DataFrame with rounding_adjustment field populated
    """
    df['rounding_adjustment'] = value
    return df


def set_base_rounding_adjustment(df: DataFrame, value: Union[int, float] = 1) -> DataFrame:
    """
    Set base rounding adjustment value.
    
    Args:
        df: Input DataFrame
        value: Value to set for base_rounding_adjustment (default: 1)
        
    Returns:
        DataFrame: DataFrame with base_rounding_adjustment field populated
    """
    df['base_rounding_adjustment'] = value
    return df


def map_payment_terms(df: DataFrame, customer_pay_terms_df: DataFrame) -> DataFrame:
    """
    Map payment terms based on due type and due days.
    
    Args:
        df: Main DataFrame
        customer_pay_terms_df: DataFrame containing payment terms mapping
        
    Returns:
        DataFrame: DataFrame with payment_terms_template field populated
    """
    df['due_key'] = (
        df['DueType'].astype(str) + '_' + 
        df['DueDays'].astype(str)
    )
    customer_pay_terms_df['due_key'] = (
        customer_pay_terms_df['DueType'].astype(str) + '_' + 
        customer_pay_terms_df['DueDays'].astype(str)
    )

    pay_terms_mapping = customer_pay_terms_df.set_index('due_key')['ERPNext Payment Terms']
    
    df['payment_terms_template'] = df['due_key'].map(pay_terms_mapping)
    
    df.drop(columns=['due_key'], inplace=True)
    
    return df


def set_terms_and_conditions(df: DataFrame, tc_name: str = 'Sales Order Acknowledgment') -> DataFrame:
    """
    Set terms and conditions name.
    
    Args:
        df: Input DataFrame
        tc_name: Terms and conditions name (default: 'Sales Order Acknowledgment')
        
    Returns:
        DataFrame: DataFrame with tc_name field populated
    """
    df['tc_name'] = tc_name
    return df


def map_order_status(df: DataFrame, document_type: str) -> DataFrame:
    """
    Map order status codes to standardized status names.
    
    Args:
        df: Input DataFrame
        document_type: Type of document ('sales_orders', 'delivery_notes', 'sales_invoices')
        
    Returns:
        DataFrame: DataFrame with standardized status field populated
    """
    if document_type == 'sales_invoices':
        status_map_standardized = {
            "J": "Cancelled",
            "C": "Closed", 
            "D": "To Bill",
            "I": "Draft",
            "N": "Draft",
            "E": "Draft",
            "O": "To Deliver and Bill",
            "P": "Completed",
            "S": "To Deliver and Bill",
        }
    else:  # sales_orders and delivery_notes use same mapping
        status_map_standardized = {
            "J": "Cancelled",
            "C": "Closed",
            "D": "To Bill", 
            "I": "Draft",
            "N": "Draft",
            "E": "Draft",
            "O": "To Deliver and Bill",
            "P": "Completed",
            "S": "To Deliver and Bill",
        }

    df['status'] = df['Status'].map(status_map_standardized)
    return df


def set_base_in_words(df: DataFrame, value: Union[int, float] = 0) -> DataFrame:
    """
    Set base amount in words value.
    
    Args:
        df: Input DataFrame
        value: Value to set for base_in_words (default: 0)
        
    Returns:
        DataFrame: DataFrame with base_in_words field populated
    """
    df['base_in_words'] = value
    return df


def set_instructions(df: DataFrame, source_column: str = 'OtherInfo') -> DataFrame:
    """
    Set instructions field from source column.
    
    Args:
        df: Input DataFrame  
        source_column: Source column for instructions (default: 'OtherInfo')
        
    Returns:
        DataFrame: DataFrame with instructions field populated
    """
    df['instructions'] = df[source_column]
    return df


def set_expected_receipt_date(df: DataFrame, source_column: str = 'dtToArrive') -> DataFrame:
    """
    Set expected receipt date field from source column.
    
    Args:
        df: Input DataFrame
        source_column: Source column for expected receipt date (default: 'dtToArrive')
        
    Returns:
        DataFrame: DataFrame with expected_receipt_date field populated
    """
    df['expected_receipt_date'] = df[source_column]
    return df


def split_returns_from_delivery_notes(order_line_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Split order lines into delivery and return components based on ShipQty.
    
    Args:
        order_line_df: DataFrame containing order line data
        
    Returns:
        Tuple[DataFrame, DataFrame]: Delivery and return order line dataframes
    """
    order_line_df.dropna(subset=['ShipQty'], inplace=True)
    order_line_delivery = order_line_df[order_line_df['ShipQty'].astype(float) > 0]
    order_line_return = order_line_df[order_line_df['ShipQty'].astype(float) < 0]
    return order_line_delivery, order_line_return


def generate_document_name(df: DataFrame, document_type: str, return_note: bool = False) -> DataFrame:
    """
    Generate standardized names for documents.
    
    Args:
        df: Input DataFrame
        document_type: Type of document ('sales_orders', 'delivery_notes', 'sales_invoices')
        return_note: Whether this is a return note (adds 'R' prefix)
        
    Returns:
        DataFrame: DataFrame with __newname field populated
    """
    # Define prefixes for each document type
    prefix_map = {
        'sales_orders': 'HSO',
        'delivery_notes': 'HDN', 
        'sales_invoices': 'HSI'
    }
    
    prefix = prefix_map.get(document_type, 'HSO')
    
    df['__newname'] = np.where(
        df['Branch'] == '01',
        f"{prefix}-AC-" + df['OrderNum'].astype(str),
        f"{prefix}-PG-" + df['OrderNum'].astype(str)
    )
    
    if return_note:
        df['__newname'] = 'R' + df['__newname']

    return df


def calculate_grand_total(df: DataFrame) -> DataFrame:
    """
    Calculate grand total from base grand total and conversion rate.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: DataFrame with grand_total field populated
    """
    df['grand_total'] = df['base_grand_total'] / df['conversion_rate']
    return df


def build_child_tables(
    main_df: DataFrame,
    sales_taxes_and_charges_df: DataFrame,
    tax_codes_df: DataFrame,
    order_line_df: DataFrame,
    add_info_df: DataFrame,
    imported_items_df: DataFrame,
    uom_map_df: DataFrame,
    country_map_df: DataFrame,
    document_type: str,
    sales_order_items_df: Optional[DataFrame] = None,
    delivery_note_items_df: Optional[DataFrame] = None,
    log: Optional[Logger] = None,
) -> DataFrame:
    """
    Build child tables for documents including items, taxes, and sales team.
    """

    if log is None:
        log = get_logger()

    def fix_date(value):
        """Fix invalid dates by replacing 9999 dates with 2099-12-31."""
        try:
            value_str = str(value)
            if value_str.startswith("9999"):
                return "2099-12-31"
            return str(pd.to_datetime(value, errors="coerce").date())
        except Exception:
            return "2099-12-31"

    def fix_list_column(val):
        """Ensure column contains valid list values."""
        return val if isinstance(val, list) else []

    def safe_strip(val):
        """Safely strip string values."""
        return str(val).strip() if pd.notna(val) else ""

    # === Merge Tax Codes ===
    order_line_df = order_line_df.merge(
        tax_codes_df,
        how="left",
        on=["Branch", "TaxCode1", "TaxCode2", "TaxCode3", "TaxCode4"],
    )

    order_line_df.drop_duplicates(
        subset=["OrderNum", "Branch", "Release", "ItemNum", "Source","OrderQty"],
        inplace=True,
    )
    # === Sort and Group Taxes ===
    sorted_taxes = (
        sales_taxes_and_charges_df.sort_values(by="account_head", ascending=True)
        .groupby("parent")
    )

    # === Prepare Mapping Dictionaries ===
    uom_map = uom_map_df.set_index("QUANTUS Code")["UOM in ERPNext"].to_dict()
    country_map = country_map_df.set_index("Quantus Country Code")[
        "ERPNext Country"
    ].to_dict()

    # === Prepare Additional Info (Message Concatenation) ===
    message_chunks = add_info_df[add_info_df["Type"] == "A"].copy()

    grouped_messages = (
        message_chunks.sort_values(by="UniqueNo")
        .groupby(["Source", "Branch", "OrderNum", "Release", "SoUniqueNo"])["Message"]
        .apply(lambda msgs: " ".join(msgs.dropna().astype(str)))
        .reset_index()
        .rename(columns={"Message": "MessageConcat"})
    )
    order_line_df['SoUniqueNo'] = order_line_df['UniqueNo']
    order_line_df = order_line_df.merge(
        grouped_messages,
        how="left",
        on=["Source", "Branch", "OrderNum", "Release","SoUniqueNo"],
    )

    order_line_df.drop_duplicates(
        subset=["OrderNum", "Branch", "Release", "ItemNum", "Source","OrderQty"],
        inplace=True,
    )

    order_line_df["description"] = order_line_df.apply(
        lambda row: f"{safe_strip(row.get('AltItemDesc'))} {safe_strip(row.get('MessageConcat'))}".strip(),
        axis=1,
    )

    # === Filter Valid Items ===
    valid_items_df = order_line_df[
        (order_line_df["ItemNum"].isin(imported_items_df["ItemNum"]))
        & (order_line_df["Source"].isin(["S", "C"]))
    ]

    valid_items_df = valid_items_df.merge(
        imported_items_df[["ItemNum", "item_code"]],
        how="left",
        on="ItemNum",
    )

    tax_data = []
    item_data = []

    grouped_orders = list(
    valid_items_df.sort_values(["OrderNum", "Branch", "LineSeqNo"])
    .groupby(["OrderNum", "Branch"])
)
    total_groups = len(grouped_orders)

    for idx, ((order_num, branch), group) in enumerate(grouped_orders, 1):
        items = []
        taxes = []

        for _, row in group.iterrows():
            conversion_rate = main_df.loc[
                (main_df["OrderNum"] == order_num)
                & (main_df["Branch"] == branch),
                "conversion_rate",
            ]
            if not conversion_rate.empty:
                converstion_rate_of_the_order = float(conversion_rate.iloc[0])
            else:
                converstion_rate_of_the_order = 1.0
            if converstion_rate_of_the_order == 0:
                log.warning(
                    f"Conversion rate is zero for OrderNum={order_num}, Branch={branch}. Defaulting to 1.0"
                )
                converstion_rate_of_the_order = 1.0

            # === Tax Aggregation ===
            template_name = row["Sales Tax Template"]
            account_head_totals = defaultdict(float)
            account_head_code = defaultdict(str)

            if template_name in sorted_taxes.groups:
                tax_rows = sorted_taxes.get_group(template_name)
                for i, (_, tax_row) in enumerate(tax_rows.iterrows(), 1):
                    ah = tax_row["account_head"]
                    amt = float(row.get(f"TaxAmt{i}", 0) or 0)
                    account_head_totals[ah] += amt
                    account_head_code[ah] = row.get(f"TaxCode{i}", "")

            if document_type == "sales_invoices":
                for ah, total_amt in account_head_totals.items():
                    existing = next(
                        (t for t in taxes if t["account_head"] == ah), None
                    )
                    if existing:
                        existing["amount"] += total_amt
                    else:
                        taxes.append(
                            {
                                "charge_type": "Actual",
                                "account_head": ah,
                                "tax_amount": total_amt,
                                "description": account_head_code[ah],
                            }
                        )
            else:
                taxes.extend(
                    {
                        "charge_type": "Actual",
                        "account_head": ah,
                        "tax_amount": amt,
                        "description": account_head_code[ah],
                    }
                    for ah, amt in account_head_totals.items()
                )

            # === Item Entry ===
            if pd.notna(row["item_code"]):
                try:
                    if document_type == "sales_orders":
                        qty = float(row["OrderQty"] or 0) - float(
                            row["ShipQty"] or 0
                        )
                        total_qty = float(row["OrderQty"] or 0)
                    else:  # delivery_notes and sales_invoices
                        qty = float(row["ShipQty"] or 0)

                    unit_price = float(row["UnitPrice"] or 0)
                    unit_cost = float(row["UnitCost"] or 0)
                    total_ext = float(row["TotalExt"] or 0)
                    disc_factor = float(row["DiscFactor"] or 0)
                    picked_qty = float(row["PickQty"] or 0)
                    valuation_rate = float(row["TotalCostExt"] or 0)
                    weight = float(row["Weight"] or 0)
                    total_weight = float(row["TotalWeight"] or 0)
                    # delivery_date = row["dtRequested"]
                    rev_map = {  # Comment this line to import to ArrowT or ArrowS
                        'F91331000': 'F91331000 Rev 0',
                        'F10512902': 'F10512902 Rev 0',
                        'F91331100': 'F91331100 Rev 0',
                        'F61260001': 'F61260001 Rev 0',
                        'F10513500': 'F10513500 Rev 0',
                        'F90008270': 'F90008270 Rev 0',
                        'F91330600': 'F91330600 Rev 0',
                        'F61001206': 'F61001206 Rev 0',
                        'F91531400': 'F91531400 Rev 0',
                        'F91530121': 'F91530121 Rev 0',
                        'F61260032': 'F61260032 Rev 0',
                        'F91330900': 'F91330900 Rev 0',
                        'F61001204': 'F61001204 Rev 0',
                        'F61009002': 'F61009002 Rev 0',
                        'F61001205' :'F61001205 Rev 0',
                        'F61009035': 'F61009035 Rev 0',
                        'F11303662': 'F11303662 Rev 0',
                        'F61260060': 'F61260060 Rev 0',
                        'F10505250': 'F10505250 Rev 0',
                        'F66682960': 'F66682960 Rev 0',

                    }

                    row["item_code"] = rev_map.get(row["item_code"], row["item_code"])
                    item_dict = {
                        "item_code": row["item_code"] ,
                        "order_line_number": int(row["LineSeqNo"]) if pd.notna(row["LineSeqNo"]) else None,
                        # "delivery_date": delivery_date,
                        "description": row.get("description", ""),
                        "country_of_origin": country_map.get(
                            row["CountryOfOrigin"], ""
                        ),
                        "qty": qty,
                        "picked_qty": picked_qty,
                        "uom": uom_map.get(row["OrderUofM"], ""),
                        "discount_percentage": disc_factor,
                        "rate": unit_price,
                        # "base_amount": total_ext/ converstion_rate_of_the_order if total_ext else 0.0,
                        "is_free_item": 1 if unit_price == 0 else 0,
                        "valuation_rate": valuation_rate,
                        "weight_per_unit": weight,
                        "total_weight": total_weight,
                        "weight_uom": uom_map.get(row["WeightUofM"], ""),
                    }

                   
                    if item_dict['qty'] != 0:
                        items.append(item_dict)

                except Exception as e:
                    log.error(
                        f"Row {idx} (OrderNum={order_num}) has invalid item data: {e}"
                    )

        # === Remove Duplicates per Order ===
        unique_items = [dict(t) for t in {tuple(sorted(d.items())) for d in items}]
        unique_taxes = [dict(t) for t in {tuple(sorted(d.items())) for d in taxes}]

        tax_data.append(
            {"OrderNum": order_num, "Branch": branch, "taxes": fix_list_column(unique_taxes)}
        )
        item_data.append(
            {
                "OrderNum": order_num,
                "Branch": branch,
                "items": fix_list_column(unique_items),
                "no_items_in_order": int(not fix_list_column(unique_items)),
            }
        )

        log.progress(f"Processed {idx}/{total_groups} orders: OrderNum={order_num}, Branch={branch}")
    # === Merge Once at End ===
    tax_df = pd.DataFrame(tax_data).drop_duplicates(subset=["OrderNum", "Branch"])
    item_df = pd.DataFrame(item_data).drop_duplicates(subset=["OrderNum", "Branch"])

    result_df = main_df.merge(tax_df, on=["OrderNum", "Branch"], how="left")
    result_df = result_df.merge(item_df, on=["OrderNum", "Branch"], how="left")

    result_df["items"] = result_df["items"].apply(fix_list_column)
    result_df["taxes"] = result_df["taxes"].apply(fix_list_column)

    if "transaction_date" in result_df.columns:
        result_df["transaction_date"] = result_df["transaction_date"].apply(fix_date)

    # === Sales Team ===
    def extract_unique_sales_team(row):
        sales_people = list(
            {row.get("SalesPerson"), row.get("SalesPerson2"), row.get("SalesPerson3")}
            - {None, "", "NA"}
        )
        total_people = len(sales_people)
        if total_people == 0:
            return []
        allocated = round(100 / total_people, 2)
        remainder = 100 - (allocated * total_people)
        sales_team = []
        for i, person in enumerate(sales_people):
            percentage = allocated + remainder if i == 0 else allocated
            sales_team.append(
                {"sales_person": person, "allocated_percentage": percentage}
            )
            if i == 0:
                remainder = 0
        return sales_team

    result_df["sales_team"] = result_df.apply(extract_unique_sales_team, axis=1)



    def drop_duplicate_dicts_in_lists(df, column, dt_col=None):
            """
            Removes duplicate dictionaries from list-type column values in a DataFrame.
            If column is 'items' and dt_col is provided, sets delivery_date to the value in dt_col for that row.
            Ensures delivery_date is formatted using pandas like format_date_field.
            If column is 'items', sorts the list by order_line_number in ascending order.
            
            Args:
                df (pd.DataFrame): DataFrame containing a column of lists of dicts.
                column (str): Name of the column to process.
                dt_col (str, optional): Name of the column containing the delivery date for each row.
                
            Returns:
                pd.DataFrame: DataFrame with duplicates removed (and delivery_date updated/formatted if applicable).
            """
            def serialize_for_json(obj):
                # Convert non-serializable types to serializable ones
                if isinstance(obj, Decimal):
                    return float(obj)
                return str(obj) if not isinstance(obj, (int, float, str, bool, type(None))) else obj


            def format_date_value(value):
                """
                Format a date value using pandas like in format_date_field.
                Invalid or missing dates default to '2099-12-31'.
                """
                if pd.isna(value):
                    return '2099-12-31'
                try:
                    dt = pd.to_datetime(value, errors='coerce')
                    if dt is pd.NaT:
                        return '2099-12-31'
                    return str(dt.date())
                except Exception:
                    return '2099-12-31'


            def remove_duplicates(row):
                lst = row[column]
                if not isinstance(lst, list):
                    return lst  # Skip non-list entries
                seen = set()
                unique = []
                for d in lst:
                    if not isinstance(d, dict):
                        continue  # Skip non-dict entries


                    # Update delivery_date if this is the 'items' column
                    if column == "items" and dt_col is not None:
                        d['delivery_date'] = format_date_value(row[dt_col])
                    else:
                        # Also format any existing delivery_date
                        if 'delivery_date' in d:
                            d['delivery_date'] = format_date_value(d['delivery_date'])


                    # Convert dict to JSON string with serializable values for hashing
                    key = json.dumps(d, sort_keys=True, default=serialize_for_json)
                    if key not in seen:
                        seen.add(key)
                        unique.append(d)
                
                # Sort by order_line_number if this is the 'items' column
                if column == "items":
                    unique = sorted(unique, key=lambda x: int(x.get('order_line_number', 0)))
                
                return unique


            df[column] = df.apply(remove_duplicates, axis=1)
            return df

    result_df = drop_duplicate_dicts_in_lists(result_df, "items", dt_col="dtRequested")
    result_df = drop_duplicate_dicts_in_lists(result_df, "taxes")


    def consolidate_dicts_in_list(df, column, group_keys, sum_key):
        """
        Groups dictionaries in a list column by specific keys and sums another key's values.
        
        Args:
            df (pd.DataFrame): DataFrame containing a column of lists of dicts.
            column (str): Column name to process.
            group_keys (list): Keys to group by (e.g. ['account_head', 'charge_type', 'description']).
            sum_key (str): Key whose numeric values should be summed (e.g. 'amount').
        
        Returns:
            pd.DataFrame: DataFrame with grouped/summed dictionaries per list.
        """
        def consolidate(lst):
            if not isinstance(lst, list):
                return lst
            
            grouped = defaultdict(lambda: 0)
            extra_data = {}
            
            for d in lst:
                key = tuple(d.get(k) for k in group_keys)
                grouped[key] += d.get(sum_key, 0)
                # keep a copy of the latest non-sum data
                extra_data[key] = {k: d.get(k) for k in group_keys}
            
            # rebuild list
            result = []
            for key, total in grouped.items():
                new_entry = extra_data[key].copy()
                new_entry[sum_key] = round(total, 2)  # optional rounding
                result.append(new_entry)
            
            return result
        
        df[column] = df[column].apply(consolidate)
        return df

    result_df = consolidate_dicts_in_list(result_df, "taxes", ['account_head', 'charge_type', 'description'], 'tax_amount')

    return result_df



def extract_and_prepare_source_data(sources: Dict[str, DataFrame], log: Logger) -> Dict[str, DataFrame]:
    """
    Extract and prepare common source data used across all transformers.
    
    Args:
        sources: Dictionary of source dataframes
        log: Logger instance
        
    Returns:
        Dictionary containing prepared dataframes
    """
    # Extract and clean order line data
    order_line_df = clean_data(sources['DAT-00000075'])[[
        'ItemNum', 'Source', 'Branch', 'OrderNum', 'Release', 'AltItemDesc',
        'OrderQty', 'ShipQty', 'OrderUofM', 'UnitPrice', 'UnitCost', 
        'TotalExt', 'TotalCostExt', 'Weight', 'TotalWeight', 'WeightUofM',
        'TaxCode1', 'TaxCode2', 'TaxCode3', 'TaxCode4', 'TaxAmt1', 
        'TaxAmt2', 'TaxAmt3', 'TaxAmt4', 'dtActualArrival', 
        'CountryOfOrigin', 'PickQty', 'DiscFactor','LineSeqNo', 'UniqueNo'
    ]]

    # Extract customer data
    customers_df = sources['DAT-00000014'][['legacy_id', 'name']]
    
    # Extract trailer codes data
    trailer_codes_df = clean_data(sources['DAT-00000009'])[['Code', 'Description', 'Type']]

    # Extract secondary addresses data
    sec_addresses_df = clean_data(sources['DAT-00000071'])[[
        'Source', 'Type', 'OrderNum', 'Branch', 'Name1', 'Name2', 
        'Address1', 'Address2', 'City', 'Prov', 'Country'
    ]]

    # Extract customer payment terms data
    customer_pay_terms_df = clean_data(sources['DAT-00000003'])[
        ['DueType', 'DueDays', 'ERPNext Payment Terms']
    ]

    # Extract UOM mapping data
    uom_map_df = clean_data(sources['DAT-00000051'])

    # Extract customer FOB data
    customer_fob_df = clean_data(sources['DAT-00000015'])

    # Extract country mapping data
    country_map_df = clean_data(sources['DAT-00000031'])

    # Extract additional information data
    add_info_df = clean_data(sources['DAT-00000073'])[
        ['Type', 'UniqueNo', 'Source', 'Branch', 'OrderNum', 'Release', 'Message','SoUniqueNo']
    ]

    # Extract sales taxes and charges data
    sales_taxes_and_charges_df = clean_data(sources['DAT-00000128'])[['parent', 'account_head']]
    
    # Extract tax codes data
    tax_codes_df = clean_data(sources['DAT-00000011'])

    
    # Clean and filter non-variant items from source DAT-00000041
    source_41_df = sources['DAT-00000041']
    non_variant_items_41 = source_41_df.loc[
        sources['DAT-00000041']['has_variants'] != 1, 
        ['ItemNum', 'item_code']
    ]

    # Clean items from source DAT-00000032
    source_32_df = sources['DAT-00000032']
    items_32 = source_32_df[['ItemNum', 'item_code']]

    # Combine both sources and remove duplicates by item_code
    imported_items_df = pd.concat(
        [non_variant_items_41, items_32],
        ignore_index=True
    ).drop_duplicates(subset='item_code')

    return {
        'order_line_df': order_line_df,
        'customers_df': customers_df,
        'trailer_codes_df': trailer_codes_df,
        'sec_addresses_df': sec_addresses_df,
        'customer_pay_terms_df': customer_pay_terms_df,
        'uom_map_df': uom_map_df,
        'customer_fob_df': customer_fob_df,
        'country_map_df': country_map_df,
        'add_info_df': add_info_df,
        'sales_taxes_and_charges_df': sales_taxes_and_charges_df,
        'tax_codes_df': tax_codes_df,
        'imported_items_df': imported_items_df
    }


def apply_final_filtering_and_cleanup(df: DataFrame, log: Logger) -> DataFrame:
    """
    Apply final filtering and cleanup operations common to all transformers.
    
    Args:
        df: Input DataFrame
        log: Logger instance
        
    Returns:
        DataFrame: Cleaned and filtered DataFrame
    """
    # Filter out empty orders and customers
    original_df = df.copy()
    
    # Filter: Remove rows with empty customer
    df = df[df['customer'].fillna('').str.strip() != ''].reset_index(drop=True)
    dropped_empty_customer = original_df[original_df['customer'].fillna('').str.strip() == '']
    if not dropped_empty_customer.empty:
        log.info("Dropped rows with empty customer:")
        log.info(dropped_empty_customer.to_string())
    
    original_df = df.copy()
    
    # Filter: Remove rows with no items in order
    df = df[df['no_items_in_order'] == 0].reset_index(drop=True)
    dropped_no_items = original_df[original_df['no_items_in_order'] != 0]
    if not dropped_no_items.empty:
        log.info("Dropped rows with no items in order:")
        log.info(dropped_no_items.to_string())
    
    original_df = df.copy()
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['__newname', 'company'], keep='last')
    dropped_duplicates = original_df[original_df.duplicated(subset=['__newname', 'company'], keep='last')]
    if not dropped_duplicates.empty:
        log.info("Dropped duplicate rows:")
        log.info(dropped_duplicates.to_string())

    return df


# Wrapper functions with original names for backward compatibility
def get_sales_orders(df: DataFrame, document_type: str = 'sales_orders') -> DataFrame:
    """Filter TransType == 'S' (SalesOrders only)."""
    return filter_sales_orders(df)


def get_open_orders(df: DataFrame, document_type: str) -> DataFrame:
    """Filter for open orders based on document type."""
    return filter_open_orders(df, document_type)


def get_customer(df: DataFrame, imported_customers_df: DataFrame = None) -> DataFrame:
    """Get customer information - handles both merge and direct copy scenarios."""
    if imported_customers_df is not None:
        return merge_customer_data(df, imported_customers_df)
    else:
        return set_customer_field(df, 'name')


def get_fob(df: DataFrame, fob_df: DataFrame) -> DataFrame:
    """Merge FOB information with main dataframe."""
    return merge_fob_data(df, fob_df)


def get_deliver_via(df: DataFrame, tailer_codes: DataFrame) -> DataFrame:
    """Map Carrier code to Description."""
    return map_delivery_method(df, tailer_codes)


def get_order_type(df: DataFrame) -> DataFrame:
    """Set order type to 'Sales' for all orders."""
    return set_order_type(df)


def get_company(df: DataFrame) -> DataFrame:
    """Map Branch to company name."""
    return map_company_by_branch(df)


def get_skip_delivery_note(df: DataFrame, document_type: str) -> DataFrame:
    """Set skip delivery note flag based on document type."""
    skip_value = 1 if document_type == 'sales_invoices' else 0
    return set_skip_delivery_note(df, skip_value)


def get_transaction_date(df: DataFrame) -> DataFrame:
    """Map dtOrder to transaction_date."""
    return format_date_field(df, 'dtOrder', 'transaction_date')


def get_delivery_date(df: DataFrame) -> DataFrame:
    """Map dtRequested to delivery_date."""
    return format_date_field(df, 'dtRequested', 'delivery_date')


def get_po_no(df: DataFrame) -> DataFrame:
    """Copy customer PO number to po_no field."""
    return copy_field(df, 'CustPoNum', 'po_no')


def get_cost_center(df: DataFrame) -> DataFrame:
    """Map branch codes to cost center names."""
    return map_cost_center_by_branch(df)


def get_drop_ship_order(df: DataFrame) -> DataFrame:
    """Flag rows with 'DROP' ShipCust as drop ship orders."""
    return set_drop_ship_order_flag(df)


def get_drop_ship_address(df: DataFrame, sec_addresses_df: DataFrame) -> DataFrame:
    """Construct drop ship addresses for orders with 'DROP' ShipCust."""
    return construct_drop_ship_address(df, sec_addresses_df)


def get_currency(df: DataFrame) -> DataFrame:
    """Determine currency and price list based on forex and branch information."""
    return determine_currency_info(df)


def get_conversion_rate(df: DataFrame) -> DataFrame:
    """Set conversion rate based on forex and exchange rate information."""
    return calculate_conversion_rate(df)


def get_set_warehoue(df: DataFrame) -> DataFrame:
    """Placeholder function for warehouse setting."""
    pass


def get_set_warehouse(df: DataFrame) -> DataFrame:
    """Placeholder function for warehouse setting."""
    pass


def get_discount_amount(df: DataFrame) -> DataFrame:
    """Set discount amount from DiscAmt field."""
    return set_discount_amount(df)


def get_base_grand_total(df: DataFrame) -> DataFrame:
    """Set base grand total from TotalAmt field."""
    return set_base_grand_total(df)


def get_rounding_adjustment(df: DataFrame) -> DataFrame:
    """Set rounding adjustment to 0 for all orders."""
    return set_rounding_adjustment(df)


def get_base_rounding_adjustment(df: DataFrame) -> DataFrame:
    """Set base rounding adjustment to 1 for all orders."""
    return set_base_rounding_adjustment(df)


def get_payement_terms_template(df: DataFrame, customer_pay_terms_df: DataFrame) -> DataFrame:
    """Map payment terms based on due type and due days."""
    return map_payment_terms(df, customer_pay_terms_df)


def get_tc_name(df: DataFrame) -> DataFrame:
    """Set terms and conditions name."""
    return set_terms_and_conditions(df)


def get_order_lost_reason_and_status(df: DataFrame, document_type: str) -> DataFrame:
    """Map order status codes to standardized status names."""
    return map_order_status(df, document_type)


def get_base_in_words(df: DataFrame) -> DataFrame:
    """Set base amount in words to 0 for all orders."""
    return set_base_in_words(df)


def get_instructions(df: DataFrame) -> DataFrame:
    """Set instructions from OtherInfo field."""
    return set_instructions(df)


def get_expected_receipt_date(df: DataFrame) -> DataFrame:
    """Set expected receipt date from dtToArrive field."""
    return set_expected_receipt_date(df)


def get_name(df: DataFrame, document_type: str, return_note: bool = False) -> DataFrame:
    """Generate standardized names for documents."""
    return generate_document_name(df, document_type, return_note)


def get_grand_total(df: DataFrame) -> DataFrame:
    """Calculate grand total from base grand total and conversion rate."""
    return calculate_grand_total(df)


def build_sales_order_child_tables(
    main_df: DataFrame,
    sales_taxes_and_charges_df: DataFrame,
    tax_codes_df: DataFrame,
    order_line_df: DataFrame,
    add_info_df: DataFrame,
    imported_items_df: DataFrame,
    uom_map_df: DataFrame,
    country_map_df: DataFrame,
    document_type: str,
    sales_order_items_df: Optional[DataFrame] = None,
    delivery_note_items_df: Optional[DataFrame] = None,
    log: Optional[Logger] = None,
) -> DataFrame:
    """Build child tables for documents including items, taxes, and sales team."""
    return build_child_tables(
        main_df=main_df,
        sales_taxes_and_charges_df=sales_taxes_and_charges_df,
        tax_codes_df=tax_codes_df,
        order_line_df=order_line_df,
        add_info_df=add_info_df,
        imported_items_df=imported_items_df,
        uom_map_df=uom_map_df,
        country_map_df=country_map_df,
        document_type=document_type,
        sales_order_items_df=sales_order_items_df,
        delivery_note_items_df=delivery_note_items_df,
        log=log
    )


def build_sales_invoice_child_tables(
    main_df: DataFrame,
    sales_taxes_and_charges_df: DataFrame,
    tax_codes_df: DataFrame,
    order_line_df: DataFrame,
    add_info_df: DataFrame,
    imported_items_df: DataFrame,
    uom_map_df: DataFrame,
    country_map_df: DataFrame,
    sales_order_items_df: DataFrame,
    delivery_note_items_df: DataFrame,
    log: Logger,
) -> DataFrame:
    """Build child tables for sales invoices including items, taxes, and sales team."""
    return build_child_tables(
        main_df=main_df,
        sales_taxes_and_charges_df=sales_taxes_and_charges_df,
        tax_codes_df=tax_codes_df,
        order_line_df=order_line_df,
        add_info_df=add_info_df,
        imported_items_df=imported_items_df,
        uom_map_df=uom_map_df,
        country_map_df=country_map_df,
        document_type='sales_invoices',
        sales_order_items_df=sales_order_items_df,
        delivery_note_items_df=delivery_note_items_df,
        log=log
    )