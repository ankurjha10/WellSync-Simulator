import time
import math
import random
import requests
import threading
import json
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer

KAFKA_BROKER = "localhost:9092"

def fetch_active_well_id():
    try:
        response = requests.get("http://localhost:8080/api/v1/wells")
        if response.status_code == 200:
            wells = response.json()
            if len(wells) > 0:
                return wells[0]["id"]
    except Exception as e:
        print(f"Failed to fetch wells from backend: {e}")
    return None

class WellPhysicsSimulator:
    def __init__(self):
        self.tick = 0
        self.scenario = "NORMAL"
        self.temperature_c = 145.0
        self.pressure_psi = 850.0
        self.viscosity_cp = 150.0
        self.base_pump_rpm = 6.0
        self.pump_rpm = 6.0
        self.stroke_length = 72.0
        self.rod_load_lbs = 11000.0
        self.steam_pressure = 0.0
        self.steam_temp = 0.0
        self.production_rate = 35.0

    def set_scenario(self, scenario):
        color = "\033[92m" if scenario in ["NORMAL", "RECOVERY"] else "\033[91m"
        print(f"\n{color}>>> [PHYSICS ENGINE] SCENARIO SHIFT: {scenario} <<<\033[0m\n")
        self.scenario = scenario

    def advance_physics(self):
        self.tick += 1
        wave = math.sin(self.tick * 0.1)
        noise = random.uniform(-1.0, 1.0)
        
        if self.scenario == "NORMAL":
            self.temperature_c = 145.0 + (wave * 0.5)
            self.viscosity_cp = 150.0 + (noise * 5)
            self.rod_load_lbs = 11000.0 + (wave * 50)
            self.pump_rpm = self.base_pump_rpm + (noise * 0.1)
        elif self.scenario == "COOLING":
            self.temperature_c = max(40.0, self.temperature_c - 0.5) 
            self.viscosity_cp = min(1200.0, self.viscosity_cp + 25.0) 
            self.rod_load_lbs = min(16000.0, self.rod_load_lbs + 150.0)
            self.pump_rpm = 11.0
        elif self.scenario == "CRITICAL":
            self.temperature_c = 45.0 + (wave * 0.2)
            self.viscosity_cp = 1150.0 + (noise * 20)
            self.rod_load_lbs = 16500.0 + (wave * 300)
            self.pump_rpm = 12.0
        elif self.scenario == "RECOVERY":
            if self.pump_rpm > self.base_pump_rpm:
                self.pump_rpm = max(self.base_pump_rpm, self.pump_rpm - 1.0)
            elif self.pump_rpm < self.base_pump_rpm:
                self.pump_rpm = min(self.base_pump_rpm, self.pump_rpm + 1.0)
                
            self.temperature_c = min(150.0, self.temperature_c + 2.0)
            self.viscosity_cp = max(150.0, self.viscosity_cp - 50.0)
            self.rod_load_lbs = max(10000.0, self.rod_load_lbs - 500.0)
            if self.temperature_c >= 140.0:
                self.set_scenario("NORMAL")
        elif self.scenario == "EMERGENCY_STOP":
            self.pump_rpm = 0.0
            self.rod_load_lbs = max(0.0, self.rod_load_lbs - 2000.0)
            self.production_rate = 0.0

    def get_telemetry_payload(self, well_id):
        return {
            "wellId": well_id,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "temperatureC": round(self.temperature_c, 2),
            "pressurePsi": round(self.pressure_psi, 2),
            "viscosityCp": round(self.viscosity_cp, 2),
            "pumpRpm": round(self.pump_rpm, 2),
            "rodLoadLbs": round(self.rod_load_lbs, 2),
            "spm": round(self.pump_rpm, 2),
            "vfdFrequencyHz": round(self.pump_rpm * 6, 2),
            "strokeLengthIn": round(self.stroke_length, 2),
            "pumpEfficiencyPercent": round(max(30.0, 90.0 - (self.viscosity_cp / 50.0)), 2),
            "steamPressurePsi": round(self.steam_pressure, 2),
            "steamTemperatureC": round(self.steam_temp, 2),
            "productionRateBopd": round(self.production_rate * (self.pump_rpm/6.0) if self.pump_rpm > 0 else 0, 2)
        }

def command_listener(simulator):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "simulator-edge-group",
        "auto.offset.reset": "latest"
    })
    consumer.subscribe(["control.commands"])
    print("🎧 Started Kafka Listener for control.commands")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue
                
            cmd = json.loads(msg.value().decode("utf-8"))
            cmd_type = cmd.get("commandType")
            cmd_val = cmd.get("requestedValue")
            
            print(f"\n" + "="*60)
            print(f"\033[91m🚨 [SCADA EDGE OVERRIDE] KAFKA COMMAND RECEIVED! \033[0m")
            print(f"\033[93m>>> EXECUTING ACTION:\033[0m {cmd_type} -> \033[92m{cmd_val}\033[0m")
            print("="*60 + "\n")
            
            if cmd_type == "SET_RPM":
                simulator.base_pump_rpm = float(cmd_val)
                if simulator.scenario != "NORMAL":
                    simulator.set_scenario("RECOVERY")
            elif cmd_type == "SET_STEAM_RATE":
                simulator.steam_pressure = float(cmd_val)
                simulator.steam_temp = 250.0
                if simulator.scenario != "NORMAL":
                    simulator.set_scenario("RECOVERY")
            elif cmd_type == "STOP_PUMP":
                simulator.pump_rpm = 0.0
                simulator.production_rate = 0.0
                simulator.set_scenario("EMERGENCY_STOP")
            elif cmd_type == "START_PUMP":
                simulator.base_pump_rpm = float(cmd_val) if cmd_val else 6.0
                simulator.production_rate = 35.0
                simulator.set_scenario("NORMAL")
    finally:
        consumer.close()

if __name__ == "__main__":
    print("=========================================")
    print("  WellSync AI - Kafka Edge Simulator     ")
    print("=========================================")
    well_id = fetch_active_well_id()
    if not well_id: exit(1)
    
    simulator = WellPhysicsSimulator()
    simulator.set_scenario("NORMAL")
    
    listener_thread = threading.Thread(target=command_listener, args=(simulator,), daemon=True)
    listener_thread.start()
    
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    print("📡 Started Kafka Producer for telemetry.raw")
    
    try:
        while True:
            simulator.advance_physics()
            payload = simulator.get_telemetry_payload(well_id)
            
            color = "\033[92m" if simulator.scenario in ["NORMAL", "RECOVERY"] else "\033[91m"
            print(f"\033[94m[{payload['timestamp']}]\033[0m Tick={simulator.tick:04d} | {color}{simulator.scenario:8s}\033[0m | RPM=\033[93m{payload['pumpRpm']:>5.2f}\033[0m | Load={payload['rodLoadLbs']:>8.2f} lbs")
            
            producer.produce("telemetry.raw", key=well_id, value=json.dumps(payload))
            producer.poll(0)
                
            if simulator.scenario != "EMERGENCY_STOP":
                if simulator.tick == 15: simulator.set_scenario("COOLING")
                elif simulator.tick == 35: simulator.set_scenario("CRITICAL")
                
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nSimulator stopped.")
        producer.flush()
