import argparse
import json
import os
import time
import keyboard
import pynmea2

version = "1.0"  # Define the version of script


def extract_lat_lon(msg):
    if isinstance(msg, pynmea2.GGA):
        return {
            "count": extract_lat_lon.count,
            "app": "gps_test",
            "timestamp": int(time.time()),
            "latitude": "{:.4f}".format(msg.latitude),
            "longitude": "{:.4f}".format(msg.longitude)
        }
    return None


extract_lat_lon.count = 1  # Initialize count to 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--debug", action="store_true", help="Enable debug messages")
    ap.add_argument('-v', '--version', action='store_true', help="Print version number")
    ap.add_argument("-n", "--lines", type=int, help="Number of lines to display")
    args = ap.parse_args()

    if args.version:
        print(f"NMEA to JSON Converter Version {version}")  # Print the version and exit
        return

    if args.debug:
        print("Debug mode enabled.")

    filename = "assignment_1.nmea"

    if not os.path.isfile(filename):
        print("Error: File not found.")
        return

    try:
        with open(filename, 'r') as nmea_file:
            print("File opened successfully.")

            if args.lines is not None:
                lines_to_display = args.lines
            else:
                lines_to_display = float('inf')  # Default to displaying all lines

            delay = input("Enter delay in seconds for each line (0 for no delay): ")
            try:
                delay = int(delay)
            except ValueError:
                print("Invalid input. Using no delay.")
                delay = 0

            keyboard.add_hotkey('q', exit_program)  # Register the 'q' key as a hotkey to exit

            lines_displayed = 0  # Initialize a counter for displayed lines

            for line in nmea_file:
                if extract_lat_lon.count > lines_to_display:
                    break

                try:
                    msg = pynmea2.parse(line)
                    location_data = extract_lat_lon(msg)
                    if location_data:
                        print(json.dumps(location_data))
                        extract_lat_lon.count += 1  # Increment count for each valid line
                        lines_displayed += 1  # Increment the displayed lines counter

                        # Add a delay based on the input
                        if delay > 0:
                            time.sleep(delay)

                except pynmea2.ParseError:
                    if args.debug:
                        print(f"Warning: Skipping invalid NMEA sentence: {line}")

                if lines_displayed >= lines_to_display:
                    break  # Exit the loop when the desired number of lines is reached

    except Exception as e:
        print(f"An error occurred: {str(e)}")


def exit_program():
    print("Quitting gracefully...")
    exit(0)


if __name__ == '__main__':
    main()
