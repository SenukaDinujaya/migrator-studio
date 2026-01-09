from pandas import DataFrame
from sga_migrator.sga_migrator.doctype.transformer.transformers.data_cleaning_utils import clean_data
import pandas as pd

def get_dynamic_link_child_table(df, doctype):
	def create_dynamic_link(row):
		return [{"link_doctype": doctype, "link_name": row["name"]}]
	df["links"] = df.apply(create_dynamic_link, axis=1)
	return df

def replace_contactname(contact_df: pd.DataFrame, primary_contact_field: str, secondary_contact_field: str, modified_contact_field: str) -> pd.DataFrame:
	"""
	Replaces the primary contact with the secondary contact if conditions are met.

	Args:
		contact_df (pd.DataFrame): The contact DataFrame.
		primary_contact_field (str): The field name for the primary contact name.
		secondary_contact_field (str): The field name for the secondary contact name.
		modified_contact_field (str): The name of the column to store the modified contact.

	Returns:
		pd.DataFrame: The updated DataFrame with replaced contacts.
	"""
	# Replace primary contact with secondary contact based on phone match
	contact_df.loc[
		(contact_df[primary_contact_field] == "")
		& (contact_df[secondary_contact_field] != "")
		& (contact_df["MailPhone"] != "")
		& (contact_df["ShipPhone"] != "")
		& (contact_df["CleanMailPhone"] == contact_df["CleanShipPhone"]),
		modified_contact_field,
	] = contact_df.loc[
		(contact_df[primary_contact_field] == "")
		& (contact_df[secondary_contact_field] != "")
		& (contact_df["MailPhone"] != "")
		& (contact_df["ShipPhone"] != "")
		& (contact_df["CleanMailPhone"] == contact_df["CleanShipPhone"]),
		secondary_contact_field,
	]

	# Replace primary contact with secondary contact based on cell phone match
	contact_df.loc[
		(contact_df[primary_contact_field] == "")
		& (contact_df[secondary_contact_field] != "")
		& (contact_df["MailCellPhone"] != "")
		& (contact_df["ShipCellPhone"] != "")
		& (contact_df["CleanMailCellPhone"] == contact_df["CleanShipCellPhone"]),
		modified_contact_field,
	] = contact_df.loc[
		(contact_df[primary_contact_field] == "")
		& (contact_df[secondary_contact_field] != "")
		& (contact_df["MailCellPhone"] != "")
		& (contact_df["ShipCellPhone"] != "")
		& (contact_df["CleanMailCellPhone"] == contact_df["CleanShipCellPhone"]),
		secondary_contact_field,
	]

	# Replace primary contact with secondary contact based on email match
	contact_df.loc[
		(contact_df[primary_contact_field] == "")
		& (contact_df[secondary_contact_field] != "")
		& (contact_df["MailEmail"] != "")
		& (contact_df["ShipEmail"] != "")
		& (contact_df["MailEmail"] == contact_df["ShipEmail"]),
		modified_contact_field,
	] = contact_df.loc[
		(contact_df[primary_contact_field] == "")
		& (contact_df[secondary_contact_field] != "")
		& (contact_df["MailEmail"] != "")
		& (contact_df["ShipEmail"] != "")
		& (contact_df["MailEmail"] == contact_df["ShipEmail"]),
		secondary_contact_field,
	]

	return contact_df


def get_mailcontacts(contact_df, mailcontact_person_fieldname, shipcontact_person_fieldname=None):

	mailcontact_df = contact_df.copy(deep = True)

	mailcontact_df['modified_mail_contact'] = mailcontact_df[mailcontact_person_fieldname].copy(deep = True)

	mailcontact_df['is_billing_contact'] = 1

	cust_mailcontact_info = [mailcontact_person_fieldname, "MailEmail", "MailPhone", "MailCellPhone", "TollFreePhone"]
	mailcontact_df = mailcontact_df[~(mailcontact_df[cust_mailcontact_info] == '').all(axis=1)]

	if shipcontact_person_fieldname is not None:
		mailcontact_df = replace_contactname(mailcontact_df, mailcontact_person_fieldname, shipcontact_person_fieldname, "modified_mail_contact")
		
	# Extracting MailContact from email if MailContact is empty
	mailcontact_df = extract_name_from_email(mailcontact_df, 'MailEmail', 'name_from_email')
	mailcontact_df.loc[mailcontact_df["modified_mail_contact"] == "", "modified_mail_contact"] = mailcontact_df.loc[mailcontact_df["modified_mail_contact"] == "", "name_from_email"]

	# Make Name1 MailContact if MailContact is still empty
	mailcontact_df.loc[mailcontact_df["modified_mail_contact"] == "", "modified_mail_contact"] = mailcontact_df.loc[mailcontact_df["modified_mail_contact"] == "", "Name1"]

	# Make MailContact uppercase
	mailcontact_df['modified_mail_contact'] = mailcontact_df['modified_mail_contact'].str.upper()

	mailcontact_df = get_mailcontact_phone_child_table(mailcontact_df)
	mailcontact_df = get_mailcontact_email_child_table(mailcontact_df)

	mailcontact_df['first_name'] = mailcontact_df['modified_mail_contact']

	return mailcontact_df


def get_mailcontact_phone_child_table(df):
	def create_mailcontact_phone(row):
		phone_data = []
		
		# Add MailPhone if it's not empty
		if row["MailPhone"] != "":
			if row["MailPhone"] != row["MailCellPhone"]:
				phone_data.append({"phone": row["MailPhone"], "is_primary_phone": 1, "is_primary_mobile_no": 0})
			elif row["MailPhone"] == row["MailCellPhone"]:
				phone_data.append({"phone": row["MailPhone"], "is_primary_phone": 1, "is_primary_mobile_no": 1})
		
		# Add a new line for MailCellPhone if it's not empty and not same as MailPhone
		if row["MailCellPhone"] != "" and row["MailCellPhone"] != row["MailPhone"]:
				phone_data.append({"phone": row["MailCellPhone"], "is_primary_phone": 0, "is_primary_mobile_no": 1})
			
		# Add TollFreePhone if it's not empty
		if row["TollFreePhone"] != "" and row["TollFreePhone"] != row["MailPhone"] and row["TollFreePhone"] != row["MailCellPhone"]:
				phone_data.append({"phone": row["TollFreePhone"], "is_primary_phone": 0, "is_primary_mobile_no": 0})
		
		return phone_data

	df["phone_nos"] = df.apply(create_mailcontact_phone, axis=1)

	return df

def get_mailcontact_email_child_table(df):
	def create_mailcontact_email(row):
		if row["MailEmail"] != "":
			return [{"email_id": row["MailEmail"].replace('"Krauze, Alice" <Alice.Krauze@adm.com>','Alice.Krauze@adm.com'), "is_primary": 1}]
		else:
			return []  # Return an empty list when MailEmail is empty

	df["email_ids"] = df.apply(create_mailcontact_email, axis=1)

	return df


def get_customer_shipcontacts(customer_df, mailcontact_person_fieldname, shipcontact_person_fieldname):

	customer_shipcontacts_df = customer_df.copy(deep = True)

	customer_shipcontacts_df['modified_ship_contact'] = customer_shipcontacts_df[shipcontact_person_fieldname].copy(deep = True)

	customer_shipcontacts_df['is_billing_contact'] = 0

	cust_shipcontact_info = [shipcontact_person_fieldname, "ShipEmail", "ShipPhone", "ShipCellPhone"]
	customer_shipcontacts_df = customer_shipcontacts_df[~(customer_shipcontacts_df[cust_shipcontact_info] == '').all(axis=1)]

	# Replace ShipContact with ShipContact if ShipContact is empty and MailPhone is same as ShipPhone
	customer_shipcontacts_df = replace_contactname(customer_shipcontacts_df, shipcontact_person_fieldname, mailcontact_person_fieldname, "modified_ship_contact")
	
	# Extracting ShipContact from email if ShipContact is empty
	customer_shipcontacts_df = extract_name_from_email(customer_shipcontacts_df, 'ShipEmail', 'name_from_email')
	customer_shipcontacts_df.loc[customer_shipcontacts_df["modified_ship_contact"] == "", "modified_ship_contact"] = customer_shipcontacts_df.loc[customer_shipcontacts_df["modified_ship_contact"] == "", "name_from_email"]
	
	# Make Name1 ShipContact if ShipContact is still empty
	customer_shipcontacts_df.loc[customer_shipcontacts_df["modified_ship_contact"] == "", "modified_ship_contact"] = customer_shipcontacts_df.loc[customer_shipcontacts_df["modified_ship_contact"] == "", "Name1"]
	
	# Make ShipContact uppercase
	customer_shipcontacts_df['modified_ship_contact'] = customer_shipcontacts_df['modified_ship_contact'].str.upper()

	customer_shipcontacts_df = get_shipcontact_phone_child_table(customer_shipcontacts_df)
	customer_shipcontacts_df = get_shipcontact_email_child_table(customer_shipcontacts_df)

	customer_shipcontacts_df['first_name'] = customer_shipcontacts_df['modified_ship_contact']

	return customer_shipcontacts_df

# def get_shipcontact_phone_child_table(df):
	# def create_shipcontact_phone(row):
	# 	phone_data = []
		
	# 	# Add ShipPhone if it's not empty
	# 	if row["ShipPhone"] != "":
	# 		phone_data.append({"phone": row["ShipPhone"].replace('q',''), "is_primary_phone": 0, "is_primary_mobile_no": 0})
		
	# 	# Add ShipCellPhone if it's not empty and not same as ShipPhone
	# 	if row["ShipCellPhone"] != "" and row["ShipCellPhone"] != row["ShipPhone"]:
	# 		phone_data.append({"phone": row["ShipCellPhone"].replace('q',''), "is_primary_phone": 0, "is_primary_mobile_no": 0})
	# 	print(phone_data)
	# 	return phone_data

	# df["phone_nos"] = df.apply(create_shipcontact_phone, axis=1)

def get_shipcontact_phone_child_table(df):
    phone_nos = []

    for _, row in df.iterrows():
        phone_data = []

        ship_phone = str(row.get("ShipPhone", "")).strip()
        ship_cell = str(row.get("ShipCellPhone", "")).strip()

        if ship_phone:
            phone_data.append({
                "phone": ship_phone.replace("q", ""),
                "is_primary_phone": 0,
                "is_primary_mobile_no": 0
            })

        if ship_cell and ship_cell != ship_phone:
            phone_data.append({
                "phone": ship_cell.replace("q", ""),
                "is_primary_phone": 0,
                "is_primary_mobile_no": 0
            })

        phone_nos.append(phone_data)

    df["phone_nos"] = phone_nos
    return df


# def get_shipcontact_email_child_table(df):
# 	def create_shipcontact_email(row):
# 		if row["ShipEmail"] != "":
# 			return [{"email_id": row["ShipEmail"], "is_primary": 0}]
# 		else:
# 			return []  # Return an empty list when ShipEmail is empty

# 	df["email_ids"] = df.apply(create_shipcontact_email, axis=1)

# 	return df

def get_shipcontact_email_child_table(df):
    email_ids = []

    for _, row in df.iterrows():
        emails = []

        ship_email = str(row.get("ShipEmail", "")).strip()

        if ship_email:
            emails.append({
                "email_id": ship_email,
                "is_primary": 0
            })

        email_ids.append(emails)

    df["email_ids"] = email_ids
    return df


def extract_name_from_email(df: DataFrame, email_column: str, new_column: str) -> DataFrame:
	df[new_column] = df[email_column].str.split('<').str[0]
	df[new_column] = df[new_column].str.split('@').str[0]
	df[new_column] = df[new_column].str.split('.').str[0]
	df[new_column] = df[new_column].str.replace(r'[^a-zA-Z ]', '', regex=True)

	return df

def get_erpnext_names(quantus_df, erpnext_df, doctype):
	if doctype == "Customer":
		quantus_fieldname = "CustomerAcct"
	elif doctype == "Supplier":
		quantus_fieldname = "VendorAcct"
	quantus_df = pd.merge(quantus_df, erpnext_df, left_on=quantus_fieldname, right_on="legacy_id", how="left")
	return quantus_df


def get_filtered_phone(phone_list):
	# Create a dictionary to handle the phone numbers, keeping the primary ones if there are duplicates
	phone_dict = {}
	
	for phone in phone_list:
		phone_number = phone['phone']
		
		# If the phone number is not in the dictionary, add it
		if phone_number not in phone_dict:
			phone_dict[phone_number] = phone
		else:
			# If the phone number is already in the dictionary, compare the flags to keep the correct phone number
			existing_phone = phone_dict[phone_number]
			
			# Compare is_primary_mobile_no first (if both are 1, keep existing; otherwise, set 1 to the one with higher priority)
			if phone['is_primary_mobile_no'] == 1:
				existing_phone['is_primary_mobile_no'] = 1
			
			# If mobile flag is not set, check is_primary_phone
			elif phone['is_primary_phone'] == 1:
				existing_phone['is_primary_phone'] = 1

	# Return the values of the dictionary (which are the unique, prioritized phone numbers)
	return list(phone_dict.values())

def get_primary_contact(all_contacts):

	# Create a column to mark the primary contact (initially False)
	all_contacts['is_primary_contact'] = 0

	# Iterate through each group of contacts, grouped by both 'name'
	for _, group in all_contacts.groupby(['name']):
		
		# Find the billing contacts (if any) per customer
		billing_contacts = group[group['is_billing_contact'] == 1]
		
		if not billing_contacts.empty:
			# Sort billing contacts by 'dtCreated' in descending order to find the latest one
			latest_billing_contact = billing_contacts.sort_values(by=['first_name', 'dtCreated'], ascending=[False, False]).iloc[0]
			
			# Set the latest billing contact as the primary contact
			all_contacts.loc[
				(all_contacts['name'] == latest_billing_contact['name']) & 
				(all_contacts['first_name'] == latest_billing_contact['first_name']) &
				(all_contacts['is_billing_contact'] == 1), 
				'is_primary_contact'
			] = 1
		else:
			# If no billing contact is found, select the latest contact of any type
			latest_contact = group.sort_values(by=['first_name', 'dtCreated'], ascending=[False, False]).iloc[0]
			
			# Set this latest contact as the primary contact
			all_contacts.loc[
				(all_contacts['name'] == latest_contact['name']) & 
				(all_contacts['first_name'] == latest_contact['first_name']), 
				'is_primary_contact'
			] = 1
	
	return all_contacts

def transform(sources: dict[str, DataFrame]) -> DataFrame:
	"""Transforms data from one format to another.

	Args:
		sources (dict[str, DataFrame]): A dictionary of dataframes, where the key is the name of the source datatable (i.e. DAT-00001).

	Returns:
		DataFrame: A pandas DataFrame containing the transformed data.

	"""
	
	# *********** Get required sources *********** #
	customer_df = sources["DAT-00000001"][["dtCreated", "CustomerAcct", "MailContact", "ShipContact", "Name1", "MailEmail", "ShipEmail", "ShipPhone", "ShipCellPhone",  "MailPhone", "MailCellPhone", "TollFreePhone"]]
	vendor_df = sources["DAT-00000004"][["dtCreated", "VendorAcct", "ContactPerson", "Name1", "MailEmail", "MailPhone", "MailCellPhone", "TollFreePhone"]]
	erpnext_customer_df = sources["DAT-00000014"][["name", "legacy_id"]]
	erpnext_supplier_df = sources["DAT-00000017"][["name", "legacy_id"]]
	# *********** END: Get required sources *********** #

	# *********** Clean required sources *********** #
	customer_df = clean_data(customer_df)
	vendor_df = clean_data(vendor_df)
	erpnext_customer_df = clean_data(erpnext_customer_df)
	erpnext_supplier_df = clean_data(erpnext_supplier_df)

	# Remove non-numeric characters (e.g., hyphens)
	customer_df['CleanMailPhone'] = customer_df['MailPhone'].str.replace(r'\D', '', regex=True)
	customer_df['CleanShipPhone'] = customer_df['ShipPhone'].str.replace(r'\D', '', regex=True)
	customer_df['CleanMailCellPhone'] = customer_df['MailCellPhone'].str.replace(r'\D', '', regex=True)
	customer_df['CleanShipCellPhone'] = customer_df['ShipCellPhone'].str.replace(r'\D', '', regex=True)
	
	customer_df['Name1'] = customer_df['Name1'].str.upper()
	vendor_df['Name1'] = vendor_df['Name1'].str.upper()
	# *********** END: Clean required sources *********** #

	customer_df = get_erpnext_names(customer_df, erpnext_customer_df, "Customer")
	vendor_df = get_erpnext_names(vendor_df, erpnext_supplier_df, "Supplier")

	# Exception for customers
	# GENERAL MILLS #GA000070
	# customer_df.loc[customer_df['ShipPhone'] == '612-770-5334 PLANT MGR', 'ShipPhone'] = '612-770-5334'
	# FAIRFIELD RICE #CP00012
	customer_df.loc[customer_df['MailPhone'] == '592-258-0671/2', 'MailPhone'] = '592-258-0671'
	# GRANT'S SEEDGRADING #GA000237
	# customer_df.loc[customer_df['MailPhone'] == '0428321367/0350321367', 'MailPhone'] = '0428321367'
	# SESACO CORPORATION #GA000151
	# customer_df.loc[customer_df['TollFreePhone'] == 'MAIN OFFICE 512-389-0759', 'TollFreePhone'] = '512-389-0759'

	# Exception for supplier ANTHONY W. PETERSON #GV000215
	vendor_df.loc[vendor_df['MailPhone'] == '763-784-6011?', 'MailPhone'] = '763-784-6011'

	# *********** Transform required sources *********** #
	# Get Customer contacts
	customer_mailcontact_df = get_mailcontacts(customer_df, "MailContact", "ShipContact")
	customer_shipcontact_df = get_customer_shipcontacts(customer_df, "MailContact", "ShipContact")
	all_customer_contacts = pd.concat([customer_mailcontact_df, customer_shipcontact_df])
	all_customer_contacts = get_dynamic_link_child_table(all_customer_contacts, 'Customer')

	# Grouping by customer and contact_name, combining phone numbers (unique), and checking the flag while merging ship and mailcontacts
	all_customer_contacts = all_customer_contacts.groupby(['name', 'first_name']).agg(
		dtCreated = ('dtCreated', 'max'),
		Name1 = ('Name1', 'first'),
		links = ('links', 'first'),
		phone_nos=('phone_nos', lambda x: sum(x, [])),
		email_ids=('email_ids', lambda x: sum(x, [])),
		is_billing_contact=('is_billing_contact', 'max')  # Keep 'True' if any flag is True in the group
	).reset_index()

	# all_customer_contacts['phone_nos'] = all_customer_contacts['phone_nos'].apply(get_filtered_phone)

	all_customer_contacts = all_customer_contacts.sort_values(by=['name'])

	print("Total customer contacts: ", len(all_customer_contacts))

	# Get vendor contacts
	vendor_mailcontact_df = get_mailcontacts(vendor_df, "ContactPerson")
	vendor_mailcontact_df = get_dynamic_link_child_table(vendor_mailcontact_df, 'Supplier')
	vendor_mailcontact_df = vendor_mailcontact_df.sort_values(by=['name'])

	# vendor_mailcontact_df['phone_nos'] = vendor_mailcontact_df['phone_nos'].apply(get_filtered_phone)

	print("Total vendor contacts: ", len(vendor_mailcontact_df))

	all_contacts = pd.concat([all_customer_contacts, vendor_mailcontact_df]).reset_index(drop=True)

	all_contacts['status'] = 'Passive'

	all_contacts = get_primary_contact(all_contacts)

	all_contacts["company_name"] = all_contacts["Name1"]

	all_contacts = all_contacts[['first_name', 'status', 'company_name', 'email_ids', 'phone_nos', 'links', 'is_primary_contact', 'is_billing_contact']]

	# all_contacts = all_contacts.head(1000)

	print("Total contacts: ", len(all_contacts))
	
	return all_contacts
