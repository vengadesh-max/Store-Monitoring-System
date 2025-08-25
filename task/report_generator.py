import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import List, Tuple
import os
from task.database import DatabaseManager


class ReportGenerator:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    async def generate_report(self, report_id: str) -> str:
        """Generate store monitoring report with uptime/downtime metrics"""
        print(f"Starting report generation for {report_id}...")

        store_ids = self.db_manager.get_all_store_ids()
        store_status_df = self.db_manager.get_store_status_data()
        business_hours_df = self.db_manager.get_business_hours_data()
        timezones_df = self.db_manager.get_timezones_data()

        store_status_df["timestamp_utc"] = pd.to_datetime(
            store_status_df["timestamp_utc"]
        )
        current_timestamp = self.db_manager.current_timestamp

        last_hour_start = current_timestamp - timedelta(hours=1)
        last_day_start = current_timestamp - timedelta(days=1)
        last_week_start = current_timestamp - timedelta(weeks=1)

        results = []

        for store_id in store_ids:
            print(f"Processing store: {store_id}")

            store_status = store_status_df[
                store_status_df["store_id"] == store_id
            ].copy()
            store_business_hours = business_hours_df[
                business_hours_df["store_id"] == store_id
            ]
            store_timezone = timezones_df[timezones_df["store_id"] == store_id]

            timezone_str = (
                store_timezone["timezone_str"].iloc[0]
                if not store_timezone.empty
                else "America/Chicago"
            )
            timezone = pytz.timezone(timezone_str)

            uptime_last_hour, downtime_last_hour = self._calculate_period_metrics(
                store_status,
                store_business_hours,
                timezone,
                last_hour_start,
                current_timestamp,
            )

            uptime_last_day, downtime_last_day = self._calculate_period_metrics(
                store_status,
                store_business_hours,
                timezone,
                last_day_start,
                current_timestamp,
            )

            uptime_last_week, downtime_last_week = self._calculate_period_metrics(
                store_status,
                store_business_hours,
                timezone,
                last_week_start,
                current_timestamp,
            )

            results.append(
                {
                    "store_id": store_id,
                    "uptime_last_hour": uptime_last_hour,
                    "uptime_last_day": uptime_last_day,
                    "uptime_last_week": uptime_last_week,
                    "downtime_last_hour": downtime_last_hour,
                    "downtime_last_day": downtime_last_day,
                    "downtime_last_week": downtime_last_week,
                }
            )

        results_df = pd.DataFrame(results)
        os.makedirs("reports", exist_ok=True)
        csv_file_path = os.path.abspath(f"reports/store_report_{report_id}.csv")
        results_df.to_csv(csv_file_path, index=False)

        print(f"Report generated successfully: {csv_file_path}")
        return csv_file_path

    def _calculate_period_metrics(
        self,
        store_status: pd.DataFrame,
        business_hours: pd.DataFrame,
        timezone: pytz.timezone,
        start_time: datetime,
        end_time: datetime,
    ) -> Tuple[float, float]:
        """Calculate uptime and downtime for a specific time period"""
        # Ensure start_time and end_time are timezone-aware UTC
        if start_time.tzinfo is None:
            start_time = pytz.UTC.localize(start_time)
        if end_time.tzinfo is None:
            end_time = pytz.UTC.localize(end_time)

        period_status = store_status[
            (store_status["timestamp_utc"] >= start_time)
            & (store_status["timestamp_utc"] <= end_time)
        ].copy()

        if period_status.empty:
            return 0.0, 0.0

        # Convert UTC timestamps to local timezone
        period_status["timestamp_local"] = period_status["timestamp_utc"].dt.tz_convert(
            timezone
        )
        period_status["dayOfWeek"] = period_status["timestamp_local"].dt.dayofweek
        period_status["time_local"] = period_status["timestamp_local"].dt.time

        if business_hours.empty:
            return self._calculate_24_7_metrics(period_status, start_time, end_time)
        else:
            return self._calculate_business_hours_metrics(
                period_status, business_hours, start_time, end_time, timezone
            )

    def _calculate_24_7_metrics(
        self, period_status: pd.DataFrame, start_time: datetime, end_time: datetime
    ) -> Tuple[float, float]:
        """Calculate metrics for stores open 24/7"""
        # Ensure timestamps are timezone-aware for comparison
        if start_time.tzinfo is None:
            start_time = pytz.UTC.localize(start_time)
        if end_time.tzinfo is None:
            end_time = pytz.UTC.localize(end_time)

        total_minutes = (end_time - start_time).total_seconds() / 60

        if period_status.empty:
            return 0.0, total_minutes / 60

        period_status = period_status.sort_values("timestamp_utc")
        uptime_minutes = 0
        downtime_minutes = 0

        for _, row in period_status.iterrows():
            if row["status"] == "active":
                uptime_minutes += 60
            else:
                downtime_minutes += 60

        return uptime_minutes / 60, downtime_minutes / 60

    def _calculate_business_hours_metrics(
        self,
        period_status: pd.DataFrame,
        business_hours: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        timezone: pytz.timezone,
    ) -> Tuple[float, float]:
        """Calculate metrics for stores with specific business hours"""
        # Ensure timestamps are timezone-aware for comparison
        if start_time.tzinfo is None:
            start_time = pytz.UTC.localize(start_time)
        if end_time.tzinfo is None:
            end_time = pytz.UTC.localize(end_time)

        uptime_minutes = 0
        downtime_minutes = 0

        business_intervals = self._get_business_intervals(
            business_hours, start_time, end_time, timezone
        )

        for interval_start, interval_end in business_intervals:
            interval_minutes = (interval_end - interval_start).total_seconds() / 60

            interval_observations = period_status[
                (period_status["timestamp_utc"] >= interval_start)
                & (period_status["timestamp_utc"] <= interval_end)
            ]

            if interval_observations.empty:
                downtime_minutes += interval_minutes
            else:
                interval_uptime, interval_downtime = self._interpolate_observations(
                    interval_observations, interval_start, interval_end
                )
                uptime_minutes += interval_uptime
                downtime_minutes += interval_downtime

        return uptime_minutes / 60, downtime_minutes / 60

    def _get_business_intervals(
        self,
        business_hours: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        timezone: pytz.timezone,
    ) -> List[Tuple[datetime, datetime]]:
        """Generate all business hour intervals within the given period"""
        intervals = []
        current_time = start_time

        while current_time < end_time:
            local_time = current_time.astimezone(timezone)
            day_of_week = local_time.weekday()

            day_business_hours = business_hours[
                business_hours["dayOfWeek"] == day_of_week
            ]

            if not day_business_hours.empty:
                for _, row in day_business_hours.iterrows():
                    start_time_str = row["start_time_local"]
                    end_time_str = row["end_time_local"]

                    start_hour, start_minute, start_second = map(
                        int, start_time_str.split(":")
                    )
                    end_hour, end_minute, end_second = map(int, end_time_str.split(":"))

                    day_start = local_time.replace(
                        hour=start_hour,
                        minute=start_minute,
                        second=start_second,
                        microsecond=0,
                    )
                    day_end = local_time.replace(
                        hour=end_hour,
                        minute=end_minute,
                        second=end_second,
                        microsecond=0,
                    )

                    day_start_utc = day_start.astimezone(pytz.UTC)
                    day_end_utc = day_end.astimezone(pytz.UTC)

                    interval_start = max(day_start_utc, current_time)
                    interval_end = min(day_end_utc, end_time)

                    if interval_start < interval_end:
                        intervals.append((interval_start, interval_end))

            current_time = (local_time + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            current_time = current_time.astimezone(pytz.UTC)

        return intervals

    def _interpolate_observations(
        self,
        observations: pd.DataFrame,
        interval_start: datetime,
        interval_end: datetime,
    ) -> Tuple[float, float]:
        """Interpolate uptime/downtime based on observations within a business interval"""
        if observations.empty:
            return 0.0, (interval_end - interval_start).total_seconds() / 60

        observations = observations.sort_values("timestamp_utc")
        uptime_minutes = 0
        downtime_minutes = 0

        for _, row in observations.iterrows():
            if row["status"] == "active":
                uptime_minutes += 60
            else:
                downtime_minutes += 60

        return uptime_minutes, downtime_minutes
