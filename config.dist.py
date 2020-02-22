# Read registers every x seconds.
INTERVAL = 2

# When an error occurred when reading the registers and the inverter was in
# waiting state, Wait x seconds.
WAIT_SECS = 300

# The difference between now and the inverter time may not exceed x seconds.
# Do not use a value lower than 5 seconds here since it takes a couple of
# seconds to set the date and time.
MAX_TIME_DELTA_SECS = 30

# Read data from this port.
PORT = '/dev/ttyUSB0'

# MQTT settings
MQTT_USER = 'mosquitto'
MQTT_PASSWORD = 'mypassword'
MQTT_HOST = "ip_or_hostname"
MQTT_PORT = 8883
MQTT_TOPIC = "monitoring/solar"
