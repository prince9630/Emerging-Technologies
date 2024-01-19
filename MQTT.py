import json
import os
import sys
import argparse
import pynmea2
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder
import time
import keyboard

# Global variable to control the main loop
exit_main = True

# Path to the .NMEA file
nmea_file_path = r"C:\Users\princ\PycharmProjects\pythonProject2\Assignment_2\gps_data.nmea"

# Function to handle 'Q' key events


def on_key_event(e):
    global exit_main
    if e.name == 'q' and e.event_type == keyboard.KEY_DOWN:
        print("The 'Q' key has been pressed. Exiting...")
        exit_main = False  # To exit the main loop

# Listen for the 'Q' key directly


keyboard.on_press(on_key_event)

# Function to send data to MQTT


def mqtt_send_data(args):
    if args.version:
        print("Application Version: 1.0.0")
        sys.exit(0)

    serial_number = "Prince"
    endpoint = "a1rfvk7lvovzao-ats.iot.ca-central-1.amazonaws.com"
    keep_alive_secs = 60
    topic = "dt/conestoga/esd/lab/" + serial_number

    ca_filepath = r"C:\Users\princ\PycharmProjects\pythonProject2\Assignment_2\Certificates\AmazonRootCA1.pem"
    cert_filepath = r"C:\Users\princ\PycharmProjects\pythonProject2\Assignment_2\Certificates\certificate.pem.crt.crt"
    key_filepath = r"C:\Users\princ\PycharmProjects\pythonProject2\Assignment_2\Certificates\private.pem.key.key"

    # Spin up resources
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=endpoint,
        cert_filepath=cert_filepath,
        pri_key_filepath=key_filepath,
        client_bootstrap=client_bootstrap,
        ca_filepath=ca_filepath,
        client_id=serial_number,
        clean_session=False,
        keep_alive_secs=keep_alive_secs
    )

    if os.path.exists(nmea_file_path):
        count = 1
        t_flag = True
        if args.debug:
            print("Debug mode is enabled.")
        pub_period = args.pub_period if args.pub_period > 0 else 1

        if args.debug:
            print(f"Publish period set to: {pub_period} seconds")

        with open(nmea_file_path, 'r') as file:
            if args.debug:
                print("Opened the NMEA file successfully")

            print("Connecting to the MQTT server...")

            try:
                # Make connect() call
                connect_future = mqtt_connection.connect()
                # Future.result() waits until a result is available
                connect_future.result()
            except:
                print("Failed to connect to the MQTT server. Device: {}".format(serial_number))
                return

            print("Connected to the MQTT server successfully!")
            print("MQTT Topic: " + topic)

            line_count = 0

            for line in file:
                if line.startswith('$'):
                    try:
                        msg = pynmea2.parse(line)

                        if hasattr(msg, 'latitude') and hasattr(msg, 'longitude'):
                            location_data = {
                                "count": count,
                                "app": "group_3",
                                "timestamp": int(time.time()),
                                "latitude": "{:.4f}".format(msg.latitude),
                                "longitude": "{:.4f}".format(msg.longitude)
                            }
                            if args.debug:
                                print("Location Data retrieved")

                            if t_flag and exit_main:
                                mqtt_connection.publish(topic=topic, payload=json.dumps(location_data),
                                                        qos=mqtt.QoS.AT_LEAST_ONCE)
                                print("Published Location Data: ", location_data)

                                if args.debug:
                                    print("Data published successfully")
                                count += 1

                            t_flag = False
                            t1 = time.time()
                            while not time.time() - t1 > pub_period and exit_main:
                                t_flag = True
                            if not exit_main:
                                print("Gracefully quitting the program")
                                sys.exit(0)

                            line_count += 1

                            if args.lines is not None and line_count >= args.lines:
                                print("Maximum lines displayed. Exiting...")
                                break
                    except pynmea2.ParseError:
                        print(f"Failed to parse line: {line}")

    else:
        print(f"The file at {nmea_file_path} does not exist")
        directory_path = os.path.dirname(nmea_file_path)

        if os.path.exists(directory_path):
            files = os.listdir(directory_path)
            print(f"Available Files under {directory_path}:\n")

            for file in files:
                if file.endswith("nmea"):
                    print(file)

            print("\nCheck the above file names and correct your file path.")
        else:
            print(f"Parent Directory {directory_path} does not exist")
        sys.exit(0)

    print("Disconnecting from MQTT...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected from MQTT!")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('-v', '--version', action='store_true', help="Display the application version (1.0.0)")
    ap.add_argument('-d', '--debug', action='store_true', help="Enable debug messages")
    ap.add_argument('-p', '--pub_period', type=int, default=1, help="Publish period in seconds")
    ap.add_argument('-n', '--lines', type=int, help="Number of lines to display")
    args = ap.parse_args()

    mqtt_send_data(args)
