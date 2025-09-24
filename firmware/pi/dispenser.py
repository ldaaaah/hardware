import json, time, threading, os
import RPi.GPIO as GPIO
import paho.mqtt.client as mqtt

CFG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
with open(CFG_PATH) as f:
    CFG = json.load(f)

DEVICE_UUID = CFG["deviceUuid"]
BROKER = CFG["broker"].replace("tcp://","").split(":")[0]
LINES = {int(k):v for k,v in CFG["lines"].items()}
PULSE = {int(k):float(v) for k,v in CFG["pulse_sec"].items()}
TOPIC_RX = CFG["topic_rx"].replace("DEVICE_UUID", DEVICE_UUID)
TOPIC_TX = CFG["topic_tx"].replace("DEVICE_UUID", DEVICE_UUID)

GPIO.setmode(GPIO.BCM)
for pin in LINES.values():
    GPIO.setup(pin, GPIO.OUT)
    GPIO.output(pin, GPIO.HIGH)

def pump_once(line:int):
    pin = LINES[line]
    GPIO.output(pin, GPIO.LOW)
    time.sleep(PULSE[line])
    GPIO.output(pin, GPIO.HIGH)

def run_plan(plan, req_id):
    try:
        for item in plan:
            line = int(item["line"]); cnt = int(item["count"])
            for _ in range(cnt):
                pump_once(line)
                time.sleep(0.15)
        client.publish(TOPIC_TX, json.dumps({
            "reqId": req_id, "deviceUuid": DEVICE_UUID,
            "state": "done",
            "detail": {f"line{p['line']}": p["count"] for p in plan}
        }), qos=1)
    except Exception as e:
        client.publish(TOPIC_TX, json.dumps({
            "reqId": req_id, "deviceUuid": DEVICE_UUID,
            "state": "error", "message": str(e)
        }), qos=1)

def on_connect(client, userdata, flags, rc, props=None):
    client.subscribe(TOPIC_RX, qos=1)

def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode())
    req_id = data.get("reqId","")
    plan = data["plan"]
    threading.Thread(target=run_plan, args=(plan, req_id), daemon=True).start()
    client.publish(TOPIC_TX, json.dumps({
        "reqId": req_id, "deviceUuid": DEVICE_UUID, "state":"running"
    }), qos=1)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, 1883, 60)
client.loop_forever()
