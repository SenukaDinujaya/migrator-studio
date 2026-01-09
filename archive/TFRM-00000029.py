
from pandas import DataFrame
from sga_migrator.sga_migrator.doctype.transformer.transformers.data_cleaning_utils import clean_data, get_logger
import pandas as pd

    
def filter_by_branch(retail_price_df: DataFrame) -> DataFrame:
    # Keep only branches 01 and PG
    filtered_df = retail_price_df[retail_price_df["Branch"].isin(["01", "PG"])].copy()

    # Sort so that '01' comes before 'PG'
    filtered_df["Branch"] = pd.Categorical(filtered_df["Branch"], categories=["01", "PG"], ordered=True)
    filtered_df.sort_values("Branch", inplace=True)

    # Drop duplicates, keeping the preferred branch ('01')

    result_df = filtered_df.drop_duplicates(subset=["ItemNum","Branch","dtEffective"], keep="first").reset_index(drop=True)
    return result_df


def get_valid_from(retail_price_df):
    # Convert 'dtEffective' column from ISO8601 string to pandas datetime (removes timezone info)
    retail_price_df['valid_from'] = pd.to_datetime(retail_price_df['dtEffective'], utc=True).dt.tz_convert(None)
    # Format datetime to string in 'YYYY-MM-DD HH:MM:SS' format for MySQL
    retail_price_df['valid_from'] = retail_price_df['valid_from'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return retail_price_df

def get_valid_upto(retail_price_df: pd.DataFrame) -> pd.DataFrame:
    # Map dtEffective to valid_upto
    retail_price_df['valid_upto'] = retail_price_df['dtEffective']
    
    # Replace '9999' dates with '2099-12-31', otherwise convert safely
    retail_price_df['valid_upto'] = retail_price_df['valid_upto'].apply(
        lambda x: '2099-12-31 00:00:00' if str(x).startswith('9999') 
        else pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d %H:%M:%S')
    )
    return retail_price_df

def get_price_list_rate(retail_price_df):
	retail_price_df['price_list_rate'] = retail_price_df['BaseFactor1']
	return retail_price_df

def get_selling(retail_price_df):
    
	retail_price_df['selling'] = 1
	return retail_price_df

def get_customer(inventory_value_df: DataFrame, customer_df: DataFrame) -> DataFrame:
	customer_map = customer_df.set_index('legacy_id')['name'].to_dict()

	inventory_value_df['customer'] = inventory_value_df['dtEffective'].map(customer_map).fillna('')
     
	return inventory_value_df

def get_currency(quotations_df: DataFrame) -> DataFrame:
    # Map currency and price list based on ForExch and Branch
    
    def map_currency(row):
        branch = str(row.get('Branch', '')).strip()

        if branch == '01':
            row['currency'] = 'CAD'
            row['price_list_currency'] = 'CAD'
            row['price_list'] = 'Standard Selling CAD'
        elif branch == 'PG':
            row['currency'] = 'USD'
            row['price_list_currency'] = 'USD'
            row['price_list'] = 'Standard Selling USD'
        else:
            row['currency'] = ''
            row['price_list_currency'] = ''
            row['price_list'] = ''
        return row
    
    quotations_df['plc_conversion_rate'] = 1.0
    return quotations_df.apply(map_currency, axis=1)


def get_item_code(retail_price_df, imported_items_df):
    merged_df = retail_price_df.merge(
        imported_items_df[['ItemNum', 'item_code']],
        how='left',
        on=['ItemNum']
    )
    # Drop rows where item_code is missing
    merged_df = merged_df.dropna(subset=['item_code'])
    return merged_df

def transform(sources: dict[str, DataFrame]) -> DataFrame:
    """Transforms data from one format to another.

    Args:
        sources (dict[str, DataFrame]): A dictionary of dataframes, where the key is the name of the source datatable (i.e. DAT-00001).

    Returns:
        DataFrame: A pandas DataFrame containing the transformed data.
    """
    logger = get_logger()
    logger.info('Transformation started.')

    # Clean and filter non-variant items from source DAT-00000041
    logger.info('Cleaning source DAT-00000041.')
    source_41_df = clean_data(sources['DAT-00000041'])
    source_41_df = source_41_df[source_41_df['has_variants'] == 0]

    # Clean items from source DAT-00000032
    logger.info('Cleaning source DAT-00000032.')
    source_32_df = clean_data(sources['DAT-00000032'])

    # Combine both sources and remove duplicates by item_code
    logger.info('Combining sources and dropping duplicates by item_code.')
    imported_items_df = pd.concat(
        [source_41_df, source_32_df],
        ignore_index=True
    ).drop_duplicates(subset='item_code')

    logger.info('Items imported and deduplicated.')

    # Clean retail price data
    logger.info('Cleaning source DAT-00000132 for retail prices.')
    retail_price_db = clean_data(sources['DAT-00000132'])
    retail_price_db['dtCreated'] = pd.to_datetime(retail_price_db['dtCreated'], errors='coerce')
    retail_price_db = retail_price_db[(retail_price_db['dtCreated'] > '2025-11-01') & (retail_price_db['dtCreated'] != '9999-01-01')].reset_index(drop=True)
    # Keep the row with the latest dtEffective for each (ItemNum, Branch, dtEffective) pair
    logger.info('Filtering latest retail prices by (ItemNum, Branch, dtEffective) based on dtEffective.')
    retail_price_db = (
        retail_price_db
        .sort_values('dtEffective', ascending=False)
        .drop_duplicates(subset=['ItemNum', 'Branch', 'dtEffective'], keep='first')
        .reset_index(drop=True)
    )

    retail_price_db = retail_price_db[
        pd.to_datetime(retail_price_db['dtCreated'], errors='coerce') > pd.Timestamp('2025-10-15')
    ]

    # Filter by branch
    logger.info('Filtering retail prices by branch.')
    retail_price_db = filter_by_branch(retail_price_db)
    print('Number of retail price records after branch filtering:', len(retail_price_db))

    # Map item_code from imported items
    logger.info('Mapping item_code from imported items.')
    retail_price_db = get_item_code(retail_price_db, imported_items_df)
    retail_price_db = retail_price_db[retail_price_db['item_code'].notna()|retail_price_db['item_code']!=''].reset_index(drop=True)
    print('Number of retail price records after mapping item_code:', len(retail_price_db))
    # logger.info('Getting Customer')
    # retail_price_db = get_customer(retail_price_db,customer_df)
    # Set currency
    logger.info('Assigning currency.')
    retail_price_db = get_currency(retail_price_db)

    # Set price list rate
    logger.info('Calculating price_list_rate.')
    retail_price_db = get_price_list_rate(retail_price_db)

    # Mark as selling
    logger.info('Marking prices as selling prices.')
    retail_price_db = get_selling(retail_price_db)

    # Set valid_from and valid_upto dates
    logger.info('Setting valid_from date.')
    retail_price_db = get_valid_from(retail_price_db)

    # logger.info('Setting valid_upto date.')
    # retail_price_db = get_valid_upto(retail_price_db)

    # Final selection of columns
    logger.info('Selecting final columns for output.')
    retail_price_db = retail_price_db[[
        'item_code',
        'price_list',
        'selling',
        'price_list_rate',
        'valid_from',
        # 'valid_upto'
    ]]

    logger.info('Transformation completed successfully.')
    print('Number of retail price records after transformation:', len(retail_price_db))
    print(retail_price_db.head(10))
    return retail_price_db