import asyncio
import aiohttp
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import logging
from typing import AsyncIterator, Dict, Any, Optional
import os
from pyspark.sql import SparkSession
from pyspark.errors import PySparkException
from pyspark.dbutils import DBUtils
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CustomerIORow:
    billing_agreement_id: int
    attributes: dict[str, Any]
    
@dataclass
class SyncResults:
    sync_success: int
    sync_failed: int
    sync_errors: list[str]
    failed_sync_agreements_list: list[Any]

class CustomerIOSyncer:
    def __init__(
        self,
        dbutils: DBUtils,
        brand_shortname: str,
        company_id: str,
        traits_table: str,
        sync_status_table: str,
        chunk_size: int = 5000,
        max_concurrent: int = 50,
        base_url: str = "https://track.customer.io/api/v1",
        spark_connection: SparkSession | None = None,
        customerio_site_id: str | None = None,
        customerio_api_key: str | None = None
    ):
        self.brand_shortname = str(brand_shortname).lower()
        self.company_id = str(company_id).upper()
        self.chunk_size = chunk_size
        self.max_concurrent = max_concurrent
        self.traits_table = traits_table
        self.sync_status_table = sync_status_table
        self.base_url = base_url
        self.customerio_site_id = customerio_site_id or dbutils.secrets.get( scope="auth_common", key=f"customerio-site-id-{self.brand_shortname}" )
        self.customerio_api_key = customerio_api_key or dbutils.secrets.get( scope="auth_common", key=f"customerio-api-key-{self.brand_shortname}" ) 
        self.spark = spark_connection or SparkSession.builder.getOrCreate()
        
        # Rate limiting
        self.rate_limit = 500
        self.request_times = []
    
    def get_customers_with_updates(self, last_sync_timestamp: Optional[datetime] = None) -> pd.DataFrame:
        """
        Fetch only customers with updates from Databricks warehouse
        """
        # Default to last hour if no timestamp provided
        if last_sync_timestamp is None:
            last_sync_timestamp = datetime.now() - timedelta(hours=1)
        
        query = f"""
        select {self.traits_table}.*
        from {self.traits_table}
        left join {self.sync_status_table} 
            on {self.traits_table}.billing_agreement_id = {self.sync_status_table}.billing_agreement_id
        where (
            {self.traits_table}.valid_from >= {self.sync_status_table}.last_sync_at 
             or {self.traits_table}.valid_from >='{last_sync_timestamp}' 
             or {self.sync_status_table}.last_sync_at is null
             or {self.sync_status_table}.last_sync_succeeded is false
            )
            and company_id = '{self.company_id}'
        """
        
        df = self.spark.sql(query).toPandas()
                
        logger.info(f"Retrieved {len(df)} customers to sync")
        return df


    def update_customers_last_synced_at_timestamps(self, start_time: datetime, failed_sync_agreements_list: list) -> tuple:

        query_merge = f"""
            merge into {self.sync_status_table}
            using {self.traits_table}
                on {self.sync_status_table}.billing_agreement_id = {self.traits_table}.billing_agreement_id
            when matched then
                update set last_sync_at = '{start_time}', last_sync_succeeded = true
            when not matched then
                insert (billing_agreement_id, last_sync_at, last_sync_succeeded) 
                values ({self.traits_table}.billing_agreement_id, '{start_time}', true)
            """

        if failed_sync_agreements_list:
            failed_sync_agreements = ",".join(failed_sync_agreements_list)

            query_update = f"""
            update {self.sync_status_table}
            set last_sync_succeeded = false
            where billing_agreement_id in ({failed_sync_agreements})
            """ 
            

        try:
            self.spark.sql(query_merge)
            if failed_sync_agreements_list:
                self.spark.sql(query_update)
            return True, None
        except PySparkException as e:
            error_msg = f"Error updating {self.sync_status_table}. Error: {e}"
            logger.exception(e)
            return False, error_msg  
        
    
    def prepare_dataframe_for_sync(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for efficient syncing
        """
        # Make a copy to avoid modifying original
        df_prepared = df.copy()
        
        # Handle datetime columns - convert to Unix timestamps
        datetime_columns = df_prepared.select_dtypes(include=['datetime64[ns]']).columns
        for col in datetime_columns:
            df_prepared[col] = df_prepared[col].apply(lambda x: int(x.timestamp()) if pd.notnull(x) else None)
        
        # Convert boolean columns to strings/integers as needed
        bool_columns = df_prepared.select_dtypes(include=['bool']).columns
        for col in bool_columns:
            df_prepared[col] = df_prepared[col].astype(str)
        
        # Ensure billing_agreement_id is string
        df_prepared['billing_agreement_id'] = df_prepared['billing_agreement_id'].astype(str)
        
        return df_prepared
    

    def dataframe_to_chunks(self, df: pd.DataFrame) -> AsyncIterator[pd.DataFrame]:
        """
        Generator that yields DataFrame chunks for memory efficiency
        """
        for i in range(0, len(df), self.chunk_size):
            yield df.iloc[i:i + self.chunk_size].copy()
    

    def df_row_to_customerio_format(self, row: pd.Series) -> CustomerIORow:
        """
        Convert a DataFrame row to Customer.io format
        """
        # Convert Series to dict and remove billing_agreement_id from attributes
        attributes = row.drop('billing_agreement_id').to_dict()
        
        # Remove any attributes that shouldn't be sent to Customer.io
        # (like internal timestamps, etc.)
        exclude_attrs = {'valid_from', 'last_synced_at'} 
        attributes = {k: v for k, v in attributes.items() if k not in exclude_attrs}
        
        return CustomerIORow(row['billing_agreement_id'],attributes)
        
    
    async def sync_dataframe_chunk(self, df_chunk: pd.DataFrame, start_time: datetime) -> SyncResults:
        """
        Sync a DataFrame chunk to Customer.io
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)
        results = SyncResults(
            sync_success=0,
            sync_failed=0,
            sync_errors=[],
            failed_sync_agreements_list=[]
        )
        
        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(self.customerio_site_id, self.customerio_api_key),
            timeout=aiohttp.ClientTimeout(total=2700) # 45 mins
        ) as session:
            
            async def sync_single_row(row):
                async with semaphore:
                    customer_data = self.df_row_to_customerio_format(row)
                    billing_agreement_id, sync_success, sync_error = await self._sync_customer_api_call(session, customer_data)
                    return billing_agreement_id, sync_success, sync_error
                
            
            # Create tasks for all rows in the chunk
            tasks = [sync_single_row(row) for _, row in df_chunk.iterrows()]
            
            # Execute with progress tracking
            completed = 0
            total = len(tasks)
            
            for coro in asyncio.as_completed(tasks):
                billing_agreement_id, sync_success, sync_error = await coro
                completed += 1
                
                if sync_success:
                    results.sync_success += 1
                else:
                    results.failed_sync_agreements_list.append(billing_agreement_id)
                    results.sync_failed += 1
                    if sync_error:
                        results.sync_errors.append(sync_error)
                
                # Log progress
                if completed % 100 == 0:
                    logger.info(f"Chunk progress: {completed}/{total}")
        
        return results
    
    async def _sync_customer_api_call(self, session: aiohttp.ClientSession, customer_data: CustomerIORow) -> tuple:
        """
        Make the actual API call to Customer.io
        """
        # Rate limiting
        await self._rate_limit_check()
        
        billing_agreement_id = customer_data.billing_agreement_id
        attributes = customer_data.attributes
        url = f"{self.base_url}/customers/{billing_agreement_id}"

        try:
            async with session.put(url, json=attributes) as response:
                if response.status == 200:
                    return billing_agreement_id, True, None
                
                else:
                    error_text = await response.text()
                    error_msg = f"Failed to sync customer {billing_agreement_id}: Status: {response.status}. Error: {error_text}"
                    logger.error(error_msg)
                    return billing_agreement_id, False, error_msg
                    
        except aiohttp.ClientError as e:
            error_msg = f"Unable to sync customer {billing_agreement_id}. {e}"
            logger.exception(e)
            return billing_agreement_id, False, error_msg
    
    async def _rate_limit_check(self):
        """Rate limiting implementation"""
        import time
        now = time.time()
        # Get all requests that happened in the last second
        self.request_times = [t for t in self.request_times if now - t < 1.0]
        
        # Configure to pause for a bit if exceeding rate limit         
        if len(self.request_times) >= self.rate_limit:
            sleep_time = 1.0 - (now - self.request_times[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        
        self.request_times.append(now)
    
    async def sync_all_customers_with_updates(self, last_sync_timestamp: Optional[datetime] = None) -> Dict:
        """
        Main method to sync all customers with updates
        """
        start_time = datetime.now()
        total_results = SyncResults(
            sync_success=0,
            sync_failed=0,
            sync_errors=[],
            failed_sync_agreements_list=[]
        )
        
        # Fetch customers with updates from Databricks
        logger.info("Fetching customers with updates from Databricks...")
        df_customers = self.get_customers_with_updates(last_sync_timestamp)
        
        if df_customers.empty:
            logger.info("No customers with updates found. Sync complete.")
            return total_results
        
        # Prepare DataFrame
        logger.info("Preparing customer data for sync...")
        df_prepared = self.prepare_dataframe_for_sync(df_customers)
        
        # Process in chunks
        total_chunks = (len(df_prepared) + self.chunk_size - 1) // self.chunk_size
        logger.info(f"Processing {len(df_prepared)} customers in {total_chunks} chunks...")
        
        chunk_num = 0
        for df_chunk in self.dataframe_to_chunks(df_prepared):
            chunk_num += 1
            logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({len(df_chunk)} customers)")
            
            chunk_results = await self.sync_dataframe_chunk(df_chunk, start_time)
            
            # Aggregate results
            total_results.sync_success += chunk_results.sync_success
            total_results.sync_failed += chunk_results.sync_failed
            total_results.sync_errors.extend(chunk_results.sync_errors)
            total_results.failed_sync_agreements_list.extend(chunk_results.failed_sync_agreements_list)
            
            logger.info(f"Chunk {chunk_num} completed: {chunk_results.sync_success} success, {chunk_results.sync_failed} failed.")
            
            # Brief pause between chunks
            if chunk_num < total_chunks:
                await asyncio.sleep(0.5)
        
        # Log final results
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info(f"""
                    
        Sync completed in {duration:.2f} seconds
        Successful syncs: {total_results.sync_success}
        Failed syncs: {total_results.sync_failed}
        Failed sync agreements: {len(total_results.failed_sync_agreements_list)}
        Rate: {total_results.sync_success / duration:.2f} customers/second

        """)

        # Update last_synced_at timestamp in Databricks
        self.update_customers_last_synced_at_timestamps(start_time=start_time, failed_sync_agreements_list=total_results.failed_sync_agreements_list)
        end_databricks_write = datetime.now()
        duration_databricks_write = (end_databricks_write - end_time).total_seconds()

        logger.info(f"""
                    
        Updated last_synced_at in {duration_databricks_write:.2f} seconds

        """)
        
        if total_results.sync_failed > 0:
            raise Exception("Some agreements failed to sync. Check the error logs.")
        
        return total_results