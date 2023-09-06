import minimalmodbus
import serial
from apscheduler.schedulers.background import BackgroundScheduler
import json
from paho.mqtt import client as mqtt_client
import time

broker = 'broker.emqx.io'
port = 1883
topic = "inastek/soil"
client_id = "inastek1337"
username = 'emqx'
password = 'public'

class Modbus():
    PH_REG = 0x6  # 1 length
    MOIS_REG = 0x12
    TEMP_REG = 0x13
    COND_REG = 0x15
    NITR_REG = 0x1E
    PHOS_REG = 0x1F
    POTS_REG = 0x20

    def __init__(self):
        self.is_connected = False
        self.instrument = None
        self.connect()
        self.scheduler = BackgroundScheduler(daemon=True)
        self.scheduler.add_job(self.connect, 'interval', seconds=10, id="connect")
        self.scheduler.start()

    def connect(self):
        try:
            if self.is_connected == False or self.instrument == None:
                instrument = minimalmodbus.Instrument('COM9', 1, mode=minimalmodbus.MODE_RTU)

                instrument.serial.baudrate = 9600
                instrument.serial.bytesize = 8
                instrument.serial.parity = minimalmodbus.serial.PARITY_EVEN
                instrument.serial.stopbits = 1
                instrument.serial.timeout = 1
                instrument.address = 1
                instrument.mode = minimalmodbus.MODE_RTU
                instrument.close_port_after_each_call = True
                instrument.clear_buffers_before_each_transaction = True

                self.instrument = instrument
                self.is_connected = True

        except:
            self.is_connected = False
            self.instrument = None


modbus = Modbus()

def readmodbus():
    # contoh membaca nilai ph
    addr_reg_ph = modbus.PH_REG
    result_ph = modbus.instrument.read_register(addr_reg_ph, 2)

    # membaca nilai moisture
    addr_reg_moisture = modbus.MOIS_REG
    result_moisture = modbus.instrument.read_register(addr_reg_moisture, 3)

    # membaca nilai temperature
    addr_reg_temp = modbus.TEMP_REG
    result_temp = modbus.instrument.read_register(addr_reg_temp, 2)

    # membaca nilai conductivity
    addr_reg_conductivity = modbus.COND_REG
    result_conductivity = modbus.instrument.read_register(addr_reg_conductivity, 3)

    # membaca nilai nitrogen
    addr_reg_nitrogen = modbus.NITR_REG
    result_nitrogen = modbus.instrument.read_register(addr_reg_nitrogen, 3)

    # membaca nilai fosfor
    addr_reg_phosphorus = modbus.PHOS_REG
    result_phosphorus = modbus.instrument.read_register(addr_reg_phosphorus, 4)

    # membaca nilai potasium
    addr_reg_potassium = modbus.POTS_REG
    result_potassium = modbus.instrument.read_register(addr_reg_potassium, 3)

    # create json_data
    read_soil = {"ph": result_ph,
                 "humidity": result_moisture,
                 "temperature": result_temp,
                 "conductivity": result_conductivity,
                 "nitrogen": result_nitrogen,
                 "phosphor": result_phosphorus,
                 "potassium": result_potassium
                 }

    # json_data
    data_soil = json.dumps(read_soil)
    return data_soil

  
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client, data_soil):
        msg = data_soil
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        time.sleep(1)


def run():
    client = connect_mqtt()
    client.loop_start()
    while True:
        data_soil = readmodbus()
        publish(client, data_soil)


if __name__ == '__main__':
    run()
