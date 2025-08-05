import rclpy
from rclpy.node import Node
from std_msgs.msg import String

import socket
import string
import time
import statistics
import sys
import serial # Import serial for SerialException

from datetime import datetime

# Assuming chafon_rfid is installed and available in the environment
from chafon_rfid.base import CommandRunner, ReaderCommand, ReaderInfoFrame, ReaderResponseFrame, ReaderType
from chafon_rfid.command import (
    G2_TAG_INVENTORY, CF_GET_READER_INFO, CF_SET_BUZZER_ENABLED, CF_SET_RF_POWER,
    CF_SET_ACCOUSTO_OPTIC_TIMES, CF_SET_WORK_MODE_18, CF_SET_WORK_MODE_288M,
)
from chafon_rfid.response import G2_TAG_INVENTORY_STATUS_MORE_FRAMES
from chafon_rfid.transport import TcpTransport
from chafon_rfid.transport_serial import SerialTransport
from chafon_rfid.uhfreader18 import G2InventoryResponseFrame as G2InventoryResponseFrame18
from chafon_rfid.uhfreader288m import G2InventoryCommand, G2InventoryResponseFrame

TCP_PORT = 6000
DELAY = 0.00

valid_chars = string.digits + string.ascii_letters

def is_marathon_tag(tag):
    tag_data = tag.epc
    return len(tag_data) == 4 and all([chr(tag_byte) in valid_chars for tag_byte in tag_data.lstrip(bytearray([0]))])

class RfidReaderNode(Node):

    def __init__(self):
        super().__init__('rfid_reader_node')
        self.publisher_ = self.create_publisher(String, '/rfid/tags', 10)
        
        self.declare_parameter('reader_address', '/dev/ttyUSB0')
        self.declare_parameter('reconnect_interval_sec', 5.0)
        self.declare_parameter('min_rssi_threshold', 215) # Adjust as needed, 0 means no filtering
        self.declare_parameter('tag_debounce_time_sec', 0.0) # Time in seconds to debounce tags

        self.reader_address = self.get_parameter('reader_address').get_parameter_value().string_value
        self.reconnect_interval_sec = self.get_parameter('reconnect_interval_sec').get_parameter_value().double_value
        self.min_rssi_threshold = self.get_parameter('min_rssi_threshold').get_parameter_value().integer_value
        self.tag_debounce_time_sec = self.get_parameter('tag_debounce_time_sec').get_parameter_value().double_value

        self.transport = None
        self.get_inventory_cmd = None
        self.frame_type = None
        self.last_read_tags = {} # For debouncing: {tag_id: last_read_timestamp}

        self.connect_and_setup_reader_loop()

        if self.transport:
            self.timer = self.create_timer(0.05, self.read_and_publish_tags)
        else:
            self.get_logger().error("Failed to initialize RFID reader. Node will not publish tags.")

    def connect_and_setup_reader_loop(self):
        connected = False
        while rclpy.ok() and not connected:
            self.get_logger().info(f"Attempting to connect to RFID reader at {self.reader_address}...")
            connected = self._connect_and_setup_reader_once()
            if not connected:
                self.get_logger().warn(f"Connection failed. Retrying in {self.reconnect_interval_sec} seconds...")
                time.sleep(self.reconnect_interval_sec)
        if connected:
            self.get_logger().info("Successfully connected and set up RFID reader.")

    def _connect_and_setup_reader_once(self):
        try:
            if self.transport and self.transport.serial.isOpen():
                self.transport.close() # Close existing connection if any

            if self.reader_address.startswith('/') or self.reader_address.startswith('COM'):
                self.transport = SerialTransport(device=self.reader_address)
            else:
                self.transport = TcpTransport(self.reader_address, reader_port=TCP_PORT)

            runner = CommandRunner(self.transport)
            reader_type = self.get_reader_type(runner)

            # Common setup
            self.set_buzzer_enabled(self.transport, False)

            if reader_type == ReaderType.UHFReader18:
                self.set_answer_mode_reader_18(self.transport)
                self.get_inventory_cmd = ReaderCommand(G2_TAG_INVENTORY)
                self.frame_type = G2InventoryResponseFrame18
                self.set_power(self.transport, 27)
            elif reader_type in (ReaderType.UHFReader288M, ReaderType.UHFReader288MP):
                self.set_answer_mode_reader_288m(self.transport)
                self.get_inventory_cmd = G2InventoryCommand(q_value=4, antenna=0x80)
                self.frame_type = G2InventoryResponseFrame
                self.set_power(self.transport, 27)
            elif reader_type in (ReaderType.UHFReader86, ReaderType.UHFReader86_1):
                self.get_inventory_cmd = G2InventoryCommand(q_value=4, antenna=0x80)
                self.frame_type = G2InventoryResponseFrame
                self.set_power(self.transport, 26)
            elif reader_type in (ReaderType.RRU9803M, ReaderType.RRU9803M_1):
                self.get_inventory_cmd = ReaderCommand(G2_TAG_INVENTORY)
                self.frame_type = G2InventoryResponseFrame18
                self.set_power(self.transport, 13)
            else:
                self.get_logger().error(f'Unsupported reader type: {reader_type}')
                self.transport = None
                return False
            return True

        except (ValueError, socket.error, serial.SerialException) as e:
            self.get_logger().error(f'Failed to connect or setup reader: {e}')
            if self.transport:
                self.transport.close()
            self.transport = None
            return False

    def read_and_publish_tags(self):
        if not self.transport or not self.transport.serial.isOpen():
            self.get_logger().warn("Reader not connected. Attempting to reconnect...")
            self.connect_and_setup_reader_loop()
            if not self.transport or not self.transport.serial.isOpen():
                return # Still not connected, give up for this cycle

        try:
            self.transport.write(self.get_inventory_cmd.serialize())
            inventory_status = None
            while inventory_status is None or inventory_status == G2_TAG_INVENTORY_STATUS_MORE_FRAMES:
                resp = self.frame_type(self.transport.read_frame())
                inventory_status = resp.result_status
                current_time = self.get_clock().now().nanoseconds / 1e9 # Current time in seconds

                for tag in resp.get_tag():
                    if not is_marathon_tag(tag):
                        continue
                    
                    tag_id = (tag.epc.lstrip(bytearray([0]))).decode('ascii')
                    rssi = tag.rssi
                    
                    # RSSI Filtering
                    if rssi < self.min_rssi_threshold:
                        self.get_logger().debug(f"Ignoring tag {tag_id} due to low RSSI: {rssi} < {self.min_rssi_threshold}")
                        continue

                    # Debouncing
                    if tag_id in self.last_read_tags and \
                       (current_time - self.last_read_tags[tag_id]) < self.tag_debounce_time_sec:
                        self.get_logger().debug(f"Debouncing tag {tag_id}. Last read {current_time - self.last_read_tags[tag_id]:.3f}s ago.")
                        continue
                    
                    self.last_read_tags[tag_id] = current_time # Update last read time

                    msg = String()
                    msg.data = f"ID: {tag_id}, RSSI: {rssi}"
                    self.publisher_.publish(msg)
                    self.get_logger().info(f'Publishing: "{msg.data}"')

        except (socket.error, ValueError, serial.SerialException) as e:
            self.get_logger().warn(f'Error during tag reading: {e}. Attempting to reconnect...')
            if self.transport:
                self.transport.close()
            self.transport = None # Mark transport as disconnected
            # Reconnection will be handled by the check at the beginning of this method in the next cycle

    def get_reader_type(self, runner):
        get_reader_info = ReaderCommand(CF_GET_READER_INFO)
        reader_info = ReaderInfoFrame(runner.run(get_reader_info))
        return reader_info.type

    def run_command(self, transport, command):
        transport.write(command.serialize())
        status = ReaderResponseFrame(transport.read_frame()).result_status
        return status

    def set_power(self, transport, power_db):
        return self.run_command(transport, ReaderCommand(CF_SET_RF_POWER, data=[power_db]))

    def set_buzzer_enabled(self, transport, buzzer_enabled):
        return self.run_command(transport, ReaderCommand(CF_SET_BUZZER_ENABLED, data=[buzzer_enabled and 1 or 0]))

    def set_answer_mode_reader_18(self, transport):
        return self.run_command(transport, ReaderCommand(CF_SET_WORK_MODE_18, data=[0]*6))

    def set_answer_mode_reader_288m(self, transport):
        return self.run_command(transport, ReaderCommand(CF_SET_WORK_MODE_288M, data=[0]))

    def destroy_node(self):
        if self.transport:
            self.transport.close()
        super().destroy_node()

def main(args=None):
    rclpy.init(args=args)
    node = RfidReaderNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()
