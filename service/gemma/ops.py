import requests
import psycopg2

from dagster import (
    op,
    job,
    fs_io_manager,
    make_values_resource,
    Field,
)
from ..utils.io_manager_path import get_io_manager_path
from utility_hub.core_tools import get_creds_from_vault

PG_HOST = '10.0.1.83'
PG_PORT = 5432
PG_DATABASE = 'an_dwh'
PG_USER = get_creds_from_vault('DWH_USER')
PG_PASSWORD = get_creds_from_vault('DWH_PASSWORD')

# Ollama API details
OLLAMA_API_URL = 'http://10.0.2.155:11434/api/generate'


def get_click_data_from_postgres(query):
    """Fetch data from PostgreSQL with a custom query"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()

        # Execute the custom query
        cursor.execute(query)
        result = cursor.fetchall()

        # Get column names dynamically from cursor.description
        columns = [desc[0] for desc in cursor.description]
        data = [dict(zip(columns, row)) for row in result]

        cursor.close()
        conn.close()
        return data

    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL or executing query: {e}")
        return []


def format_data_for_gemma(data, context):
    """Format the data into a readable string for Gemma 2 with dynamic columns"""
    if not data:
        return "No data found."

    context.log.info(data)

    formatted = "Data:\n"
    for row in data:
        # Join all key-value pairs dynamically
        formatted += "- " + ", ".join(f"{key}: {value}" for key, value in row.items()) + "\n"
    return formatted


def query_gemma2(data_text, custom_prompt):
    """Send data to Gemma 2 via Ollama API with a custom prompt"""
    prompt = f"The following is data provided for analysis:\n{data_text}\n{custom_prompt}"
    payload = {
        "model": "gemma2",
        "prompt": prompt,
        "stream": False
    }

    try:
        response = requests.post(OLLAMA_API_URL, json=payload)
        response.raise_for_status()
        result = response.json()
        print("Gemma 2 raw response:", result)
        return result.get('response', 'No response generated')
    except requests.exceptions.RequestException as e:
        print(f"Error querying Gemma 2: {e}")
        return "Error processing request"


def save_to_postgres(gemma_response):
    """Save the Gemma 2 response to PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dwh_test.gemma_response (
                id SERIAL PRIMARY KEY,
                gemma_response TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            INSERT INTO dwh_test.gemma_response (gemma_response)
            VALUES (%s)
        """, (gemma_response,))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Saved analysis: '{gemma_response}' to PostgreSQL")

    except psycopg2.Error as e:
        print(f"Error saving to PostgreSQL: {e}")


@op(required_resource_keys={'globals'})
def main_gemma(context):
    # Custom query
    custom_query = context.resources.globals["sql_query"]
    prompt = context.resources.globals["prompt"]

    # Custom prompt
    # custom_prompt = "Using this data, compare Country ID, Traffic Source ID, Price (User Currency), and Price (USD) " \
    #                 "across each Action DateDiff value, providing one sentence per Action DateDiff to describe the comparison."

    # Fetch data with custom query
    click_data = get_click_data_from_postgres(custom_query)
    context.log.info(f"Found {len(click_data)} rows of data to process")

    # Format the data
    data_text = format_data_for_gemma(click_data, context)
    # context.log.info("Formatted data:\n", data_text)

    # Get response from Gemma 2 with custom prompt
    gemma_response = query_gemma2(data_text, prompt)
    context.log.info("saved into: dwh_test.gemma_response")

    # Save to PostgreSQL
    save_to_postgres(gemma_response)


@job(resource_defs={"globals": make_values_resource(sql_query=Field(str, default_value='select 1;'),
                                                    prompt=Field(str, default_value='add 1'),
                                                    ),
                    "io_manager": fs_io_manager.configured({"base_dir": f"{get_io_manager_path()}"})},
     name='service__ask_gemma',
     description='save result into: dwh_test.gemma_response'
     )
def ask_gemma_job():
    main_gemma()
