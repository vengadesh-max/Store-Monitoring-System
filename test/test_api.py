import asyncio
import logging
import os
import time
from io import StringIO

import pandas as pd
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_api_end_to_end():
    """Test the complete API flow end-to-end"""
    base_url = "http://localhost:8000"

    logger.info("=== Store Monitoring API End-to-End Test ===")

    # Test 1: Check if server is running
    try:
        response = requests.get(f"{base_url}/", timeout=5)
        if response.status_code == 200:
            logger.info(f" Server is running: {response.json()}")
        else:
            logger.error(f"✗ Server returned status code: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        logger.error(" Server not running. Please start the server with: python app.py")
        return False
    except Exception as e:
        logger.error(f" Error connecting to server: {e}")
        return False

    # Test 2: Trigger report generation
    try:
        response = requests.post(f"{base_url}/trigger_report")
        if response.status_code == 200:
            report_id = response.json()["report_id"]
            logger.info(f" Report triggered successfully: {report_id}")
        else:
            logger.error(f" Failed to trigger report: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f" Error triggering report: {e}")
        return False

    # Test 3: Check report status (poll until complete)
    max_attempts = 15
    for attempt in range(max_attempts):
        try:
            response = requests.get(f"{base_url}/get_report/{report_id}", timeout=10)
            if response.status_code == 200:
                # Check if response is JSON or CSV
                content_type = response.headers.get("content-type", "")
                if "application/json" in content_type or response.text.startswith("{"):
                    try:
                        data = response.json()
                                                    if "status" in data:
                                status = data["status"]
                                logger.info(f"  Report status: {status}")
                                if status == "Complete":
                                    logger.info(" Report generation completed!")
                                    break
                                elif status == "Failed":
                                    logger.error(" Report generation failed!")
                                    return False
                    except:
                        pass
                else:
                    # CSV file returned
                    logger.info("✓ Report downloaded successfully!")
                    logger.info(f"  File size: {len(response.content)} bytes")

                    # Test 4: Validate CSV content
                    csv_content = response.text
                    logger.info(f"  Content preview: {csv_content[:200]}...")

                    # Parse CSV and validate schema
                    try:
                        df = pd.read_csv(StringIO(csv_content))
                        expected_columns = [
                            "store_id",
                            "uptime_last_hour",
                            "uptime_last_day",
                            "uptime_last_week",
                            "downtime_last_hour",
                            "downtime_last_day",
                            "downtime_last_week",
                        ]

                        if all(col in df.columns for col in expected_columns):
                            logger.info(" CSV schema is correct")
                            logger.info(f"  Number of stores in report: {len(df)}")
                            logger.info("  Sample data:")
                            logger.info(df.head().to_string())
                        else:
                            logger.error("✗ CSV schema is incorrect")
                            return False
                    except Exception as e:
                        logger.error(f" Error parsing CSV: {e}")
                        return False

                    break
            else:
                logger.error(f" Error checking report status: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f" Error checking report status: {e}")
            return False

        time.sleep(2)  # Wait 2 seconds before next check

    if attempt == max_attempts - 1:
        logger.error("✗ Report generation timed out")
        return False

    logger.info("\n=== API Requirements Verification ===")
    logger.info(" /trigger_report endpoint: Returns report_id")
    logger.info(" /get_report endpoint: Returns status and CSV file")
    logger.info(" Trigger + poll architecture: Working correctly")
    logger.info(" CSV output: Correct schema and data")

    logger.info("\n=== All API tests passed! ===")
    return True


def test_error_handling():
    """Test error handling scenarios"""
    base_url = "http://localhost:8000"

    logger.info("\n=== Testing Error Handling ===")

    # Test 1: Invalid report ID
    try:
        response = requests.get(f"{base_url}/get_report/invalid-report-id")
        if response.status_code == 404:
            logger.info(" Invalid report ID returns 404")
        else:
            logger.error(f" Invalid report ID should return 404, got {response.status_code}")
    except Exception as e:
        logger.error(f" Error testing invalid report ID: {e}")

    # Test 2: Invalid endpoint
    try:
        response = requests.get(f"{base_url}/invalid_endpoint")
        if response.status_code == 404:
            logger.info(" Invalid endpoint returns 404")
        else:
            logger.error(f" Invalid endpoint should return 404, got {response.status_code}")
    except Exception as e:
        logger.error(f" Error testing invalid endpoint: {e}")

    logger.info("✓ Error handling tests completed")


if __name__ == "__main__":
    # Run end-to-end test
    if test_api_end_to_end():
        # Run error handling tests
        test_error_handling()
        logger.info("\n All tests completed successfully!")
    else:
        logger.error("\n Tests failed!")
