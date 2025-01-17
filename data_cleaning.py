import pandas as pd

def standardize_columns(crimes, offense_codes):
    """
    Standardize column names and specific text fields in the datasets.
    """
    crimes.columns = crimes.columns.str.upper()
    offense_codes.columns = offense_codes.columns.str.upper()
    crimes.rename(columns={'LAT': 'LATITUDE', 'LONG': 'LONGITUDE'}, inplace=True)
    crimes[['LATITUDE', 'LONGITUDE']] = crimes[['LATITUDE', 'LONGITUDE']].astype(float)
    crimes['OCCURRED_ON_DATE'] = pd.to_datetime(crimes['OCCURRED_ON_DATE'], errors='coerce')
    text_cols = ['OFFENSE_DESCRIPTION', 'DISTRICT', 'STREET']
    for col in text_cols:
        if col in crimes.columns:
            crimes[col] = crimes[col].str.upper()
    return crimes, offense_codes


def drop_location_column_if_consistent(crimes):
    """
    Remove LOCATION column if LATITUDE/LONGITUDE matches LOCATION.
    Print rows where the values are inconsistent.
    """
    def is_consistent(row):
        # Handle NaN or zero values in LATITUDE, LONGITUDE, or LOCATION
        if ((pd.isna(row['LATITUDE']) or pd.isna(row['LONGITUDE']) or 
             (row['LATITUDE'] == 0 and row['LONGITUDE'] == 0)) and
            (pd.isna(row['LOCATION']) or row['LOCATION'] == "(0.00000000, 0.00000000)")):
            return True
        if pd.notna(row['LOCATION']):
            lat, lon = tuple(map(float, row['LOCATION'].strip("()").split(", ")))
            return (row['LATITUDE'], row['LONGITUDE']) == (lat, lon)
        return True

    # Identify inconsistent rows
    inconsistent_rows = crimes[~crimes.apply(is_consistent, axis=1)]

    # Print inconsistent rows
    if not inconsistent_rows.empty:
        print("Rows with inconsistent LATITUDE/LONGITUDE and LOCATION values:")
        print(inconsistent_rows[['LATITUDE', 'LONGITUDE', 'LOCATION']])
        print(f"Total mismatched rows: {len(inconsistent_rows)}")
    else:
        crimes.drop(columns=['LOCATION'], inplace=True)
        print("LOCATION column deleted as all rows are consistent.")

    return crimes


def drop_date_column_if_consistent(crimes):
    """
    Drop OCCURRED_ON_DATE if split fields (YEAR, MONTH, DAY_OF_WEEK) are consistent.
    """
    def is_consistent(row):
        return (row['YEAR'], row['MONTH'], row['DAY_OF_WEEK']) == (
            row['OCCURRED_ON_DATE'].year, row['OCCURRED_ON_DATE'].month, row['OCCURRED_ON_DATE'].day_name()
        )

    # Check consistency
    if crimes.apply(is_consistent, axis=1).all():
        crimes.drop(columns=['OCCURRED_ON_DATE'], inplace=True)
        print("OCCURRED_ON_DATE column deleted as split fields are consistent.")
    else:
        print("OCCURRED_ON_DATE column retained due to inconsistencies.")
    return crimes


def correct_offense_description(crimes, offense_codes):
    """
    Correct OFFENSE_DESCRIPTION in the crimes dataset based on offense_codes.
    """
    offense_codes['NAME'] = offense_codes['NAME'].str.replace('"', '').str.upper()
    # Create a dictionary for efficient mapping
    code_to_name = offense_codes.drop_duplicates(subset='CODE').set_index('CODE')['NAME'].to_dict()
    # Map OFFENSE_CODE to update OFFENSE_DESCRIPTION
    crimes['OFFENSE_DESCRIPTION'] = crimes['OFFENSE_CODE'].map(code_to_name).fillna(crimes['OFFENSE_DESCRIPTION'])
    return crimes

def fill_missing_offense_data(crimes):
    """
    Fill missing values in OFFENSE_CODE_GROUP and OFFENSE_DESCRIPTION based on OFFENSE_CODE.
    """
    code_to_group = crimes.dropna(subset=['OFFENSE_CODE_GROUP']).drop_duplicates(subset='OFFENSE_CODE') \
                        .set_index('OFFENSE_CODE')['OFFENSE_CODE_GROUP'].to_dict()
    code_to_description = crimes.dropna(subset=['OFFENSE_DESCRIPTION']).drop_duplicates(subset='OFFENSE_CODE') \
                                .set_index('OFFENSE_CODE')['OFFENSE_DESCRIPTION'].to_dict()

    # Rellenar los valores faltantes usando los diccionarios
    crimes['OFFENSE_CODE_GROUP'] = crimes['OFFENSE_CODE'].map(code_to_group).fillna(crimes['OFFENSE_CODE_GROUP'])
    crimes['OFFENSE_DESCRIPTION'] = crimes['OFFENSE_CODE'].map(code_to_description).fillna(crimes['OFFENSE_DESCRIPTION'])

    missing_group = crimes['OFFENSE_CODE_GROUP'].isna().sum()
    missing_description = crimes['OFFENSE_DESCRIPTION'].isna().sum()

    if missing_group > 0:
        print(f"Advertencia: {missing_group} valores en OFFENSE_CODE_GROUP aún están vacíos.")
    if missing_description > 0:
        print(f"Advertencia: {missing_description} valores en OFFENSE_DESCRIPTION aún están vacíos.")
    else:
        print("Todos los valores faltantes fueron rellenados correctamente.")
    
    return crimes

def fill_missing_location_data(crimes):
    """
    Validate and correct DISTRICT and STREET based on LATITUDE and LONGITUDE.
    If multiple rows share the same LATITUDE and LONGITUDE but differ in DISTRICT or STREET,
    assign the values from the first record. If DISTRICT or STREET is blank, fill it using the
    first match found with the same LATITUDE and LONGITUDE.
"""
    # Group the DataFrame by LATITUDE and LONGITUDE
    grouped = crimes.groupby(['LATITUDE', 'LONGITUDE'])

    # Function to retrieve the first non-null value in the group
    def get_first_non_null(series):
        return series.dropna().iloc[0] if not series.dropna().empty else None

    # Iterate over the groups and fix DISTRICT and STREET
    for (lat, lon), group in grouped:
        # Get the first non-null DISTRICT and STREET
        first_district = get_first_non_null(group['DISTRICT'])
        first_street = get_first_non_null(group['STREET'])

        # Update rows in the original DataFrame
        index_to_update = crimes[(crimes['LATITUDE'] == lat) & (crimes['LONGITUDE'] == lon)].index
        crimes.loc[index_to_update, 'DISTRICT'] = first_district
        crimes.loc[index_to_update, 'STREET'] = first_street

    print("DISTRICT and STREET values have been validated and updated based on LATITUDE and LONGITUDE.")
    return crimes


# Main script
def main():
    # Load datasets
    crimes = pd.read_csv("diq_boston_crimes/data/crime.csv", encoding='ISO-8859-1')
    offense_codes = pd.read_csv("diq_boston_crimes/data/offense_codes.csv", encoding='ISO-8859-1')

    crimes, offense_codes = standardize_columns(crimes, offense_codes)
    crimes = drop_location_column_if_consistent(crimes)

    crimes['YEAR'] = crimes['OCCURRED_ON_DATE'].dt.year
    crimes['MONTH'] = crimes['OCCURRED_ON_DATE'].dt.month
    crimes['DAY_OF_WEEK'] = crimes['OCCURRED_ON_DATE'].dt.day_name()
    crimes['TIME'] = crimes['OCCURRED_ON_DATE'].dt.strftime('%H:%M')
    
    crimes = drop_date_column_if_consistent(crimes)
    crimes = correct_offense_description(crimes, offense_codes)    
    #crimes = fill_missing_offense_data(crimes)    
    #crimes = fill_missing_location_data(crimes)    
    #crimes = crimes.drop_duplicates(keep='first')

    # Save cleaned datasets
    crimes.to_csv("diq_boston_crimes/data/cleaned_crimes.csv", index=False, encoding="ISO-8859-1")
    offense_codes.to_csv("diq_boston_crimes/data/cleaned_offense_codes.csv", index=False, encoding="ISO-8859-1")
    print("Data cleaning and wrangling complete. Cleaned datasets saved.")


# Run the script
if __name__ == "__main__":
    main()
