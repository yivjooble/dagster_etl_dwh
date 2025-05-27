from dotenv import load_dotenv
from etl_clickhouse.utils.dbs_con import get_clickhouse_driver_client
from dagster import Failure

load_dotenv()


# save to Clickhouse
def save_to_clickhouse(context, db_name, df, table_name, batch_size=100000, max_workers=4):
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    def insert_batch(batch_df):
        try:
            with get_clickhouse_driver_client(context, db_name) as client:
                client.insert_dataframe(
                    query=f"INSERT INTO {table_name} VALUES",
                    dataframe=batch_df,
                    settings={
                        'async_insert': 1,
                        'max_insert_threads': 4,  # Adjust based on your ClickHouse server
                        'max_threads': 4,
                        'max_insert_block_size': batch_size
                    }
                )
            return len(batch_df)
        except Exception as e:
            context.log.error(f"Error in batch insert: {str(e)}")
            raise

    try:
        total_rows = len(df)
        context.log.info(f'Starting to insert {total_rows} rows into {table_name}')
        
        # Process in batches
        rows_processed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i in range(0, len(df), batch_size):
                batch = df[i:i + batch_size]
                futures.append(executor.submit(insert_batch, batch))
                
            for future in as_completed(futures):
                rows_processed += future.result()
                if rows_processed % (batch_size * max_workers) == 0:  # Log progress
                    context.log.info(f'Progress: {rows_processed}/{total_rows} rows inserted')
        
        context.log.info(f'Successfully inserted {rows_processed} rows into {table_name}')
        return rows_processed
        
    except Exception as e:
        context.log.error(f"Error while inserting data to Clickhouse ({db_name}): {str(e)}")
        raise
