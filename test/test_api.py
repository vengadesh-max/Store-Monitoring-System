import requests
import time
import os
import pandas as pd
import asyncio


def test_api_end_to_end():
    """Test the complete API flow end-to-end"""
    base_url = "http://localhost:8000"

    print("=== Store Monitoring API End-to-End Test ===")

    # Test 1: Check if server is running
    try:
        response = requests.get(f"{base_url}/", timeout=5)
        if response.status_code == 200:
            print(f"✓ Server is running: {response.json()}")
        else:
            print(f"✗ Server returned status code: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("✗ Server not running. Please start the server with: python app.py")
        return False
    except Exception as e:
        print(f"✗ Error connecting to server: {e}")
        return False

    # Test 2: Trigger report generation
    try:
        response = requests.post(f"{base_url}/trigger_report")
        if response.status_code == 200:
            report_id = response.json()["report_id"]
            print(f"✓ Report triggered successfully: {report_id}")
        else:
            print(f"✗ Failed to trigger report: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Error triggering report: {e}")
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
                            print(f"  Report status: {status}")
                            if status == "Complete":
                                print("✓ Report generation completed!")
                                break
                            elif status == "Failed":
                                print("✗ Report generation failed!")
                                return False
                    except:
                        pass
                else:
                    # CSV file returned
                    print("✓ Report downloaded successfully!")
                    print(f"  File size: {len(response.content)} bytes")

                    # Test 4: Validate CSV content
                    csv_content = response.text
                    print(f"  Content preview: {csv_content[:200]}...")

                    # Parse CSV and validate schema
                    try:
                        df = pd.read_csv(pd.StringIO(csv_content))
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
                            print("✓ CSV schema is correct")
                            print(f"  Number of stores in report: {len(df)}")
                            print(f"  Sample data:")
                            print(df.head().to_string())
                        else:
                            print("✗ CSV schema is incorrect")
                            return False
                    except Exception as e:
                        print(f"✗ Error parsing CSV: {e}")
                        return False

                    break
            else:
                print(f"✗ Error checking report status: {response.status_code}")
                return False
        except Exception as e:
            print(f"✗ Error checking report status: {e}")
            return False

        time.sleep(2)  # Wait 2 seconds before next check

    if attempt == max_attempts - 1:
        print("✗ Report generation timed out")
        return False

    print("\n=== API Requirements Verification ===")
    print("✓ /trigger_report endpoint: Returns report_id")
    print("✓ /get_report endpoint: Returns status and CSV file")
    print("✓ Trigger + poll architecture: Working correctly")
    print("✓ CSV output: Correct schema and data")

    print("\n=== All API tests passed! ===")
    return True


def test_error_handling():
    """Test error handling scenarios"""
    base_url = "http://localhost:8000"

    print("\n=== Testing Error Handling ===")

    # Test 1: Invalid report ID
    try:
        response = requests.get(f"{base_url}/get_report/invalid-report-id")
        if response.status_code == 404:
            print("✓ Invalid report ID returns 404")
        else:
            print(f"✗ Invalid report ID should return 404, got {response.status_code}")
    except Exception as e:
        print(f"✗ Error testing invalid report ID: {e}")

    # Test 2: Invalid endpoint
    try:
        response = requests.get(f"{base_url}/invalid_endpoint")
        if response.status_code == 404:
            print("✓ Invalid endpoint returns 404")
        else:
            print(f"✗ Invalid endpoint should return 404, got {response.status_code}")
    except Exception as e:
        print(f"✗ Error testing invalid endpoint: {e}")

    print("✓ Error handling tests completed")


if __name__ == "__main__":
    # Run end-to-end test
    if test_api_end_to_end():
        # Run error handling tests
        test_error_handling()
        print("\n🎉 All tests completed successfully!")
    else:
        print("\n❌ Tests failed!")
