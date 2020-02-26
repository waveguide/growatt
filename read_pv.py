#!/usr/bin/env python

"""
Read registers from inverter and publish results to MQTT broker.

Some counters use two registers. A 'high' and 'low' value. The high value is
used when the value doesn't fit in one register. One register is 16 bits.

Documentation about the registers is taken from:
Growatt PV Inverter Modbus RS485 RTU Protocol V3.04.pdf

The inverter will turn off when the production becomes too low. The state of
the inverter will change from 'Normal' to 'Waiting'. After approx. 90 seconds
the inverter will turn off.
"""

import time
from datetime import datetime
import json
import ssl
import sys
from enum import Enum
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.exceptions import ModbusIOException
import paho.mqtt.client as mqtt
from loguru import logger

import config


# errcodes 1-23 are 'Error: (errorcode+99)
errcodes = {
    24: 'Auto Test Failed',
    25: 'No AC Connection',
    26: 'PV Isolation Low',
    27: 'Residual Current High',
    28: 'DC Current High',
    29: 'PV Voltage High',
    30: 'AV V Outrange',
    31: 'AC Freq Outrange',
    32: 'Module Hot'}


class InverterState(Enum):
    WAITING = 0
    NORMAL = 1
    FAULT = 3


def connect_mqtt():
    """ Connect to MQTT Broker and return mqtt.Client object.
    """
    mqtt_client = mqtt.Client(client_id="inverter")
    mqtt_client.username_pw_set(username=config.MQTT_USER,
                                password=config.MQTT_PASSWORD)
    ctx = ssl.create_default_context()
    mqtt_client.tls_set_context(ctx)
    mqtt_client.connect(config.MQTT_HOST, port=config.MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()

    return mqtt_client


def update_datetime_inverter(inverter):
    """ Change the date/time of the inverter to the current date/time.
    """
    now = datetime.now()

    # TODO: Updating date/time with one write_registers call won't work. Instead
    #       update them sequentially. Also checking the result of write_register
    #       always is an exception.
    #       Anyway, ignoring the result is an option here. The date and time are
    #       updated.
    inverter.write_register(50, now.second)
    inverter.write_register(49, now.minute)
    inverter.write_register(48, now.hour)
    inverter.write_register(47, now.day)
    inverter.write_register(46, now.month)
    inverter.write_register(45, now.year)

    # Update register 45, 46, 47, 48, 49 and 50 with one call.
    # rq = inverter.write_registers(
    #     45, [now.year, now.month, now.day, now.hour, now.minute, now.second])
    # if rq.isError():
    #     logger.error(f"Failed to update date/time: {rq}")
    #     raise rq

    logger.info(f"Updated inverter date/time to {now}")


def check_datetime_inverter(dt_inverter):
    dt_now = datetime.now()

    diff = int((dt_now - dt_inverter).total_seconds())

    if diff > config.MAX_TIME_DELTA_SECS:
        logger.warning(f"Difference between current time {dt_now} and inverter "
                       f"time {dt_inverter} exceeded "
                       f"{config.MAX_TIME_DELTA_SECS} seconds!")
        return False

    return True


def read_datetime_inverter(inverter):
    """ Read the current date/time from the inverter and return as datetime
    object.
    """
    rr = inverter.read_holding_registers(45, 6)
    if rr.isError():
        logger.debug(f"Failed to read holding registers: {rr}")
        raise rr

    dt = datetime(rr.registers[0], rr.registers[1], rr.registers[2],
                  rr.registers[3], rr.registers[4], rr.registers[5])

    return dt


def read_inverter(inverter, last_inverter_state):
    """ Read input registers from inverter.
    Results are returned as a dict.
    """
    data = {}

    # Read data from inverter
    # pdf says that we can't read more than 45 registers in one go.
    rr = inverter.read_input_registers(0, 33)
    if rr.isError():
        logger.debug(f"Failed to read input registers: {rr}")
        raise rr

    inverter_state = rr.registers[0]

    if (inverter_state != last_inverter_state):
        logger.info(
            f"Changed state from {InverterState(last_inverter_state)} "
            f"to {InverterState(inverter_state)}")

        if inverter_state == InverterState.FAULT.value:
            EC = inverter.read_input_registers(40, 1)
            if 1 <= EC <= 23:
                # No specific text defined
                errstr = "Error Code " + str(99+EC)
            else:
                errstr = errcodes[EC.registers[0]]
            data['err_code'] = EC
            data['err_txt'] = errstr
            logger.warning(f"Inverter FAULT code {EC}: {errstr}")

    data['inverter_state'] = inverter_state
    data['inverter_state_txt'] = InverterState(inverter_state).name

    # PV input power in watt - Delivered by panels at DC side
    data['PV_input_power'] = \
        float((rr.registers[1] << 16) + rr.registers[2]) / 10

    # String 1 / Voltage - Current - Watt at DC side
    data['PV1_input_volt'] = float(rr.registers[3]) / 10
    data['PV1_input_current'] = float(rr.registers[4]) / 10
    data['PV1_input_watt'] = \
        float((rr.registers[5] << 16) + rr.registers[6]) / 10

    # String 2 / Voltage - Current - Watt at DC side
    data['PV2_input_volt'] = float(rr.registers[7]) / 10
    data['PV2_input_current'] = float(rr.registers[8]) / 10
    data['PV2_input_watt'] = \
        float((rr.registers[9] << 16) + rr.registers[10]) / 10

    # Total delivered by inverter to net / Power - Frequency
    data['AC_watt'] = float((rr.registers[11] << 16) + rr.registers[12]) / 10
    data['AC_frequency'] = float(rr.registers[13]) / 100

    # Fase 1 AC side delivered by inverter / Voltage - Current - Watt
    data['AC1_volt'] = float(rr.registers[14]) / 10
    data['AC1_current'] = float(rr.registers[15]) / 10
    data['AC1_watt'] = float((rr.registers[16] << 16) + rr.registers[17]) / 10

    # Fase 2 (only applicable when you have a 3 fase inverter)
    #data['Vac2'] = float(rr.registers[18])/10 # L2 grid voltage
    #data['Iac2'] = float(rr.registers[19])/10 # L2 grid output current
    #data['Pac2'] = float((rr.registers[20]<<16) + rr.registers[21])/10 # L2 grid output watt

    # Fase 3 (only applicable when you have a 3 fase inverter)
    #data['Vac3'] = float(rr.registers[22])/10 # L3 grid voltage
    #data['Iac3'] = float(rr.registers[23])/10 # L3 grid output current
    #data['Pac3'] = float((rr.registers[24]<<16) + rr.registers[25])/10 # L3 grid output watt

    # Totals
    data['total_today_watt'] = \
        ((rr.registers[26] << 16) + rr.registers[27]) * 100
    data['total_all_time_watt'] = \
        ((rr.registers[28] << 16) + rr.registers[29]) * 100
    data['total_operating_secs'] = \
        int(((rr.registers[30] << 16) + rr.registers[31]) / 2)

    # Temperature in Celcius.
    data['inverter_temp'] = float(rr.registers[32]) / 10

    return data


# Change loglevel
logger.remove()
logger.add(sys.stderr, level="INFO")

logger.info("Start read_pv")
mqtt_client = connect_mqtt()

# default state upon start of this script will be 'waiting'
last_inverter_state = InverterState.WAITING.value

inverter = ModbusClient(method='rtu', port=config.PORT, baudrate=9600,
                        stopbits=1, parity='N', bytesize=8, timeout=1)
if not inverter.connect():
    logger.error("Failed to connect to modbus serial port!")
    sys.exit(1)

# Keep reading registers.
# The inverter will turn off when the output is too low. The calls to read
# the registers will raise a ModbusIOException.
while True:
    try:
        dt_inverter = read_datetime_inverter(inverter)
        if not check_datetime_inverter(dt_inverter):
            update_datetime_inverter(inverter)
            # Reconnect to inverter after updating date/time.
            inverter.close()
            inverter.connect()

        data = read_inverter(inverter, last_inverter_state)
        data['dt_now_utc'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        data['inverter_datetime'] = \
            dt_inverter.strftime("%Y-%m-%d %H:%M:%S")

        if last_inverter_state != data['inverter_state']:
            # Publish a message with the retain flag when the inverter status
            # changes.
            state = {
                "state": f"{InverterState(data['inverter_state']).name}",
                "dt_now_local": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            mqtt_client.publish(config.MQTT_TOPIC + '/state',
                                json.dumps(state),
                                retain=True)

        last_inverter_state = data['inverter_state']
        mqtt_client.publish(config.MQTT_TOPIC + '/data', json.dumps(data))

    except ModbusIOException as e:
        if last_inverter_state == InverterState.WAITING.value:
            # Sleep longer when the last known inverter state was 'waiting'.
            # It probably turned off.
            time.sleep(config.WAIT_SECS)
        else:
            logger.warning(e)
            time.sleep(config.INTERVAL)
        continue
    except Exception as e:
        logger.error(f"Exception: {e}")
        break

    time.sleep(config.INTERVAL)

mqtt_client.loop_stop()
inverter.close()

logger.info("Stopped read_pv")
