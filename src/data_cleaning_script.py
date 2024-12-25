import pandas as pd
import json
import logging
import mysql.connector
from datetime import datetime

# Setup logging
logging.basicConfig(filename="data_transform.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def load_config(config_file):
    """Load transformation configuration."""
    with open(config_file, 'r') as f:
        return json.load(f)


def validate_data(df, required_columns):
    """Validate that required columns exist in the dataset."""
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def clean_and_transform_data(df, config):
    """Apply cleaning and transformations based on configuration."""
    # Drop duplicates
    if config.get("remove_duplicates", True):
        df = df.drop_duplicates()

    # Fill missing values
    fill_na_rules = config.get("fill_na", {})
    for col, value in fill_na_rules.items():
        if col in df.columns:
            df[col] = df[col].fillna(value)

    # Rename columns
    column_mappings = config.get("rename_columns", {})
    df = df.rename(columns=column_mappings)

    # Add calculated columns
    # calculated_columns = config.get("calculated_columns", [])
    # for calc in calculated_columns:
    #     col_name, formula = calc["name"], calc["formula"]
    #     df[col_name] = eval(formula, {"df": df})

    # Filter rows
    # filters = config.get("filters", [])
    # for condition in filters:
    #     df = df.query(condition)
    return df


def save_data(df, output_file):
    """Save transformed data to a file."""
    if output_file.endswith(".csv"):
        df.to_csv(output_file, index=False)
    elif output_file.endswith(".parquet"):
        df.to_parquet(output_file, index=False)
    else:
        raise ValueError("Unsupported file format. Use .csv or .parquet")

def save_to_mysql(df, config):
    """save transformed data to MySQL db"""
    db_config = config["db_config"]
    table_name = config["sales_data"]

    #connect to MySQL
    conn = mysql.connector.connect(
        host=db_config["localhost"],
        port=db.config["3306"],
        user=db_config["root"],
        password=db_config["1234"],
        database=db_config["sales_db_new"]
    )
    cursor = conn.cursor()

    # Create table if not exists
    create_table_query = config["create_table_query"]
    cursor.execute(create_table_query)

    # Insert data into MySQL
    for _, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Data successfully loaded into MySQL database")


def main():
    # Load configuration
    config_file = r'C:\Users\riteshgupta6\PycharmProjects\Python-data-cleaning\data\config.json'
    config = load_config(config_file)

    # Load raw data
    input_file = config["input_file"]
    # print('done')
    logging.info(f"Loading raw data from {input_file}")
    if input_file.endswith(".csv"):
        raw_data = pd.read_csv(input_file)

    elif input_file.endswith(".xlsx"):
        raw_data = pd.read_excel(input_file)
    else:
        raise ValueError("Unsupported file format. Use .csv or .xlsx")

    # Validate data
    try:
        validate_data(raw_data, config["required_columns"])
    except ValueError as e:
        logging.error(e)
        return

    # Clean and transform data
    logging.info("Cleaning and transforming data...")
    try:
        transformed_data = clean_and_transform_data(raw_data, config)
    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        return

    # Save transformed data to MySQL database
    try:
        save_to_mysql(transformed_data, config)
    except Exception as e:
        logging.error(f"Error saving data to MySQL: {e}")

    # Save transformed data
    # output_file = config["output_file"]
    # logging.info(f"Saving transformed data to {output_file}")
    # try:
    #     save_data(transformed_data, output_file)
    # except Exception as e:
    #     logging.error(f"Error saving data: {e}")


if __name__ == "__main__":
    main()
