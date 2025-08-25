import sqlite3
import pandas as pd
import asyncio
from datetime import datetime
from typing import List
import os


class DatabaseManager:
    def __init__(self, db_path: str = "store_monitoring.db"):
        self.db_path = db_path
        self.current_timestamp = None

    async def initialize_database(self):
        """Initialize the database and create tables"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            cursor = conn.cursor()

            # Create store_status table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS store_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Create business_hours table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS business_hours (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_id TEXT NOT NULL,
                    dayOfWeek INTEGER NOT NULL,
                    start_time_local TEXT NOT NULL,
                    end_time_local TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Create timezones table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS timezones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_id TEXT NOT NULL,
                    timezone_str TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Create indexes for better performance
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_store_status_store_id ON store_status(store_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_store_status_timestamp ON store_status(timestamp_utc)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_business_hours_store_id ON business_hours(store_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_timezones_store_id ON timezones(store_id)"
            )

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error initializing database: {e}")
            # Try to remove the database file if it's corrupted
            if os.path.exists(self.db_path):
                try:
                    os.remove(self.db_path)
                    print(f"Removed corrupted database file: {self.db_path}")
                    # Retry initialization
                    await self.initialize_database()
                except Exception as e2:
                    print(f"Failed to remove database file: {e2}")
                    raise e

    async def load_data_from_csv(self):
        """Load data from CSV files into the database"""
        try:
            # Clear existing data
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM store_status")
            cursor.execute("DELETE FROM business_hours")
            cursor.execute("DELETE FROM timezones")

            # Load store status data
            print("Loading store status data...")
            if os.path.exists("test_data/store_status.csv"):
                store_status_df = pd.read_csv("test_data/store_status.csv")
                print("Using test data")
            else:
                store_status_df = pd.read_csv("data/store_status.csv")
                print("Using production data")
            store_status_df.to_sql(
                "store_status", conn, if_exists="append", index=False
            )

            # Set current timestamp to max timestamp in data
            self.current_timestamp = pd.to_datetime(
                store_status_df["timestamp_utc"].max()
            )
            print(f"Current timestamp set to: {self.current_timestamp}")

            # Load business hours data
            print("Loading business hours data...")
            if os.path.exists("test_data/menu_hours.csv"):
                business_hours_df = pd.read_csv("test_data/menu_hours.csv")
            else:
                business_hours_df = pd.read_csv("data/menu_hours.csv")
            business_hours_df.to_sql(
                "business_hours", conn, if_exists="append", index=False
            )

            # Load timezones data
            print("Loading timezones data...")
            if os.path.exists("test_data/timezones.csv"):
                timezones_df = pd.read_csv("test_data/timezones.csv")
            else:
                timezones_df = pd.read_csv("data/timezones.csv")
            timezones_df.to_sql("timezones", conn, if_exists="append", index=False)

            conn.commit()
            conn.close()
            print("Data loading completed!")
        except Exception as e:
            print(f"Error loading data from CSV: {e}")
            raise e

    def get_store_status_data(self, store_id: str = None) -> pd.DataFrame:
        """Get store status data from database"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        query = "SELECT store_id, status, timestamp_utc FROM store_status"
        if store_id:
            query += f" WHERE store_id = '{store_id}'"
        query += " ORDER BY timestamp_utc"

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_business_hours_data(self, store_id: str = None) -> pd.DataFrame:
        """Get business hours data from database"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        query = "SELECT store_id, dayOfWeek, start_time_local, end_time_local FROM business_hours"
        if store_id:
            query += f" WHERE store_id = '{store_id}'"

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_timezones_data(self, store_id: str = None) -> pd.DataFrame:
        """Get timezones data from database"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        query = "SELECT store_id, timezone_str FROM timezones"
        if store_id:
            query += f" WHERE store_id = '{store_id}'"

        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_all_store_ids(self) -> List[str]:
        """Get all unique store IDs"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT store_id FROM store_status")
        store_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        return store_ids
