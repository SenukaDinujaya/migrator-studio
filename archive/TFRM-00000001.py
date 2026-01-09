from pandas import DataFrame
from sga_migrator.sga_migrator.doctype.transformer.transformers.data_cleaning_utils import clean_data
import pandas as pd
import numpy as np


def get_currency(df, legacy_db_currency_fieldname, erpnext_currency_fieldname):
    """
    Maps legacy currency codes to ERPNext currency codes and fills in missing values based on branch.

    Args:
            df (pandas.DataFrame): The DataFrame containing the data.
            legacy_db_currency_fieldname (str): The name of the column in the DataFrame that contains the legacy currency codes.
            erpnext_currency_fieldname (str): The name of the column in the DataFrame where the mapped ERPNext currency codes will be stored.
    Returns:
            pandas.DataFrame: The DataFrame with the ERPNext currency codes mapped and missing values filled.
    """

    CURRENCY_MAP = {"US": "USD", 
                    "CDN": "CAD", 
                    "RCDN": "CAD"}

    df[erpnext_currency_fieldname] = (
        df[legacy_db_currency_fieldname].map(CURRENCY_MAP).fillna("")
    )

    # Fill blanks
    df.loc[
        ((df["Branch"] == "01") & (df[erpnext_currency_fieldname] == "")),
        erpnext_currency_fieldname,
    ] = "CAD"
    df.loc[
        ((df["Branch"] == "PG") & (df[erpnext_currency_fieldname] == "")),
        erpnext_currency_fieldname,
    ] = "USD"

    return df


def get_payments_terms(df, erp_dir_arrow_005):
    df = pd.merge(df, erp_dir_arrow_005, on=["DueType", "DueDays"], how="left")
    df.rename(columns={"ERPNext Payment Terms": "payment_terms"}, inplace=True)
    return df


def get_tax_category(df, erp_dir_arrow_004):
    replacements = dict(
        zip(
            zip(
                erp_dir_arrow_004["Branch"],
                erp_dir_arrow_004["TaxCode1"],
                erp_dir_arrow_004["TaxCode2"],
                erp_dir_arrow_004["TaxCode3"],
                erp_dir_arrow_004["TaxCode4"],
            ),
            zip(
                erp_dir_arrow_004["Branch"],
                erp_dir_arrow_004["Correct TaxCode1"],
                erp_dir_arrow_004["Correct TaxCode2"],
                erp_dir_arrow_004["Correct TaxCode3"],
                erp_dir_arrow_004["Correct TaxCode4"],
            ),
        )
    )

    # Replace wrong combinations with correct ones
    for old_combo, new_combo in replacements.items():
        mask = (
            (df["Branch"] == old_combo[0])
            & (df["TaxCode1"] == old_combo[1])
            & (df["TaxCode2"] == old_combo[2])
            & (df["TaxCode3"] == old_combo[3])
            & (df["TaxCode4"] == old_combo[4])
        )
        df.loc[
            mask,
            [
                "Branch",
                "Correct TaxCode1",
                "Correct TaxCode2",
                "Correct TaxCode3",
                "Correct TaxCode4",
            ],
        ] = new_combo

    erp_dir_arrow_004_correct = erp_dir_arrow_004[
        (
            erp_dir_arrow_004["Notes"]
            != "This Combination is wrong should be fixed in Quantus"
        )
    ][
        [
            "Branch",
            "Correct TaxCode1",
            "Correct TaxCode2",
            "Correct TaxCode3",
            "Correct TaxCode4",
            "Tax Category",
        ]
    ]
    erp_dir_arrow_004_correct = erp_dir_arrow_004_correct.drop_duplicates()
    df = pd.merge(
        df,
        erp_dir_arrow_004_correct,
        on=[
            "Branch",
            "Correct TaxCode1",
            "Correct TaxCode2",
            "Correct TaxCode3",
            "Correct TaxCode4",
        ],
        how="left",
    )
    df.rename(columns={"Tax Category": "tax_category"}, inplace=True)
    return df


def get_accounts_child_table(customer_df):
    def create_accounts(row):

        accounts_table = []

        if row["Name1"] == "ARROWCORP INC.":
            company = "Premier Grain Cleaner"
            account = "1203 - Accounts Receivable Interbranch USD - PG"
            accounts_table = [{"company": company, "account": account}]
        elif row["Name1"] == "PREMIER GRAIN CLEANER CO.":
            company = "ArrowCorp"
            account = "1203 - Accounts Receivable Interbranch USD - AC"
            accounts_table = [{"company": company, "account": account}]
        elif row["default_currency"] == "USD":
            company = "ArrowCorp"
            account = "1204 - Accounts Receivable USD - AC"
            accounts_table = [{"company": company, "account": account}]

        return accounts_table

    customer_df["accounts"] = customer_df.apply(create_accounts, axis=1)
    return customer_df


def get_credit_limits_child_table(customer_df):
    def create_credit_limits(row):

        credit_limit_table = []

        credit_limit = row["CreditLimit"]

        # Create an entry in the credit_limit table only if the Credit limit is not 0
        if credit_limit != 0:

            bypass_credit_limit_check = 1 if row["CheckCredit"] == "N" else 0

            # Company
            if row["Branch"] == "01":
                company = "ArrowCorp"
            elif row["Branch"] == "PG":
                company = "Premier Grain Cleaner"

            credit_limit_table = [
                {
                    "company": company,
                    "credit_limit": credit_limit,
                    "bypass_credit_limit_check": bypass_credit_limit_check,
                }
            ]

        return credit_limit_table

    customer_df["credit_limits"] = customer_df.apply(create_credit_limits, axis=1)

    return customer_df


def get_customer_details(df, instructions_df):

    instructions_df["UniqueNo"] = instructions_df["UniqueNo"].astype(int)
    instructions_df = instructions_df.sort_values(
        by=["Source", "Type", "CustVendNum", "UniqueNo"]
    )

    # Concatenate the Message for each group of Branch, Source, Type, CustVendNum to get lines for each type
    instructions_df = (
        instructions_df.groupby(["Branch", "Source", "Type", "CustVendNum"])["Message"]
        .apply("".join)
        .reset_index()
    )
    instructions_df["Message"] = instructions_df["Message"].str.strip()

    # Add specific instructions to 'Message' based on the 'Type'
    instructions_df.loc[instructions_df["Type"] == "B", "Message"] = (
        "Invoicing Instructions:\n"
        + instructions_df.loc[instructions_df["Type"] == "B", "Message"]
    )
    instructions_df.loc[instructions_df["Type"] == "P", "Message"] = (
        "Banking Instructions:\n"
        + instructions_df.loc[instructions_df["Type"] == "P", "Message"]
    )
    instructions_df.loc[instructions_df["Type"] == "S", "Message"] = (
        "Shipping Instructions:\n"
        + instructions_df.loc[instructions_df["Type"] == "S", "Message"]
    )

    # Concatenate the Message for each group of Branch, Source, CustVendNum to get all lines for each source
    instructions_df = (
        instructions_df.groupby(["Branch", "Source", "CustVendNum"])["Message"]
        .apply("\n\n".join)
        .reset_index()
    )
    instructions_df = instructions_df[(instructions_df["Source"].isin(["S", "C"]))]

    df = pd.merge(
        df,
        instructions_df,
        left_on=["Branch", "CustomerAcct"],
        right_on=["Branch", "CustVendNum"],
        how="left",
    )

    df.rename(columns={"Message": "customer_details"}, inplace=True)

    return df


def get_territory(customer_df):

    TERRITORY_MAP = {"CA": "Canada", "USA": "United States", "": "Unspecified"}

    customer_df["territory"] = (
        customer_df["MailCountry"].map(TERRITORY_MAP).fillna("International")
    )

    return customer_df


def set_default_price_list(customer_df):

    DEFAULT_PRICE_LIST_MAP = {
        "CAD": "Standard Selling CAD",
        "USD": "Standard Selling USD",
    }

    customer_df["default_price_list"] = customer_df["default_currency"].map(
        DEFAULT_PRICE_LIST_MAP
    )

    return customer_df

def get_exempt_from_sales_tax(customer_df):
	"""Set exempt_from_sales_tax to 1 if tax_category is empty, else set it to 0."""
	
	customer_df["exempt_from_sales_tax"] = np.where(
			customer_df["tax_category"] == "", 1, 0
		)
	return customer_df


def update_erpnext_customers(sources, customer_df):
    erpnext_customer_df = sources["DAT-00000014"][["name", "legacy_id"]]  # For updating
    erpnext_customer_df = clean_data(erpnext_customer_df)
    customer_df = pd.merge(customer_df, erpnext_customer_df, on="legacy_id", how="left")
    return customer_df


def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Transforms data from one format to another.

    Args:
            sources (dict[str, DataFrame]): A dictionary of dataframes, where the key is the name of the source datatable (i.e. DAT-00001).

    Returns:
            DataFrame: A pandas DataFrame containing the transformed data.
    """

	# *********** Get required sources *********** #
    # Get the customer data from the sources dictionary. 
    customer_df = sources["DAT-00000001"][
        [
            "Name1",
            "CustomerAcct",
            "MailProv",
            "MailCountry",
            "TaxNum1",
            "TaxNum2",
            "TaxCode1",
            "TaxCode2",
            "TaxCode3",
            "TaxCode4",
            "Status",
            "Branch",
            "ForExch",
            "WebPageAddress",
            "DueType",
            "DueDays",
            "CreditLimit",
            "CheckCredit",
        ]
    ]
    erp_dir_arrow_005_payment_terms = sources["DAT-00000003"][
        ["DueType", "DueDays", "ERPNext Payment Terms"]
    ]
    erp_dir_arrow_004_tax_codes = sources["DAT-00000011"]
    instructions_df = sources["DAT-00000013"]
    # *********** END: Get required sources *********** #

	# *********** Clean required sources *********** #
    customer_df = clean_data(customer_df)
    erp_dir_arrow_005_payment_terms = clean_data(erp_dir_arrow_005_payment_terms)
    erp_dir_arrow_004_tax_codes = clean_data(erp_dir_arrow_004_tax_codes)
    instructions_df = clean_data(instructions_df)
    customer_df["Name1"] = customer_df["Name1"].str.upper()
    # *********** END: Clean required sources *********** #

    # *********** Start transforming data *********** #
    # Set the customer_name
    customer_df["customer_name"] = customer_df["Name1"]

    # Set the legacy_id
    customer_df["legacy_id"] = customer_df["CustomerAcct"]

    # Set the customer_type
    customer_df["customer_type"] = "Company"

    # Set the customer_group
    customer_df["customer_group"] = "Retail"

    # Set the territory
    customer_df = get_territory(customer_df)

    # Set the tax_id
    customer_df["tax_id"] = customer_df["TaxNum1"]

    # Set the tax_exempt_number
    customer_df["tax_exempt_number"] = customer_df["TaxNum2"]

    # Set the status
    customer_df["disabled"] = np.where(customer_df["Status"] == "A", 0, 1)

    # Set the tax_category
    customer_df = get_tax_category(customer_df, erp_dir_arrow_004_tax_codes)

    # Set exempt_from_sales_tax
    customer_df = get_exempt_from_sales_tax(customer_df)

    # Set the default_currency
    customer_df = get_currency(customer_df, "ForExch", "default_currency")

    # Set the default_price_list
    customer_df = set_default_price_list(customer_df)

    # Set the website
    customer_df["website"] = customer_df["WebPageAddress"]

    # Set the payment_terms
    customer_df = get_payments_terms(customer_df, erp_dir_arrow_005_payment_terms)

    # Set the customer_details from the instructions_df
    customer_df = get_customer_details(customer_df, instructions_df)

    # Get child tables
    # Party Account child table
    customer_df = get_accounts_child_table(customer_df)
    # Credit Limits child table
    customer_df = get_credit_limits_child_table(customer_df)
    
	# *********** END: Transforming data *********** #

    customer_df = customer_df[
        [
            "customer_name",
            "legacy_id",
            "customer_type",
            "customer_group",
            "territory",
            "tax_id",
            "tax_exempt_number",
            "tax_category",
            "website",
            "exempt_from_sales_tax",
            "disabled",
            "default_currency",
            "default_price_list",
            "payment_terms",
            "customer_details",
            "accounts",
            "credit_limits",
        ]
    ]

    # # This adds the "name" to the df to update customers for the PUT request, uncomment if POST request to add customers to ERPNext
    # customer_df = update_erpnext_customers(sources, customer_df)
    print("Total Customers: ", len(customer_df))

    return customer_df
