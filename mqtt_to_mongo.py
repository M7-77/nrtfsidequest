# ═══════════════════════════════════════════════════════════════════════════
#  ReTeqFusion — MQTT to MongoDB Atlas bridge
#  Subscribes to HiveMQ TLS broker, enriches data, stores to MongoDB Atlas
#  Run: python mqtt_to_mongo.py
# ═══════════════════════════════════════════════════════════════════════════

import json
import signal
import sys
from datetime import datetime, timezone
from collections import defaultdict
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# ═══════════════════════════════════════════════════════════════════════════
#  ★ CHANGE THESE — your personal settings
# ═══════════════════════════════════════════════════════════════════════════
MONGO_URI = "mongodb+srv://mokarimdah05_db_user:A4A9mL1Edcpe5Sg1@cluster0.1rawvgd.mongodb.net/"
#            ↑ paste your full MongoDB Atlas connection string here
# ═══════════════════════════════════════════════════════════════════════════
#  These match the ESP32 sketch — only change if you changed them there too
# ═══════════════════════════════════════════════════════════════════════════
MQTT_BROKER = "broker.emqx.io"
MQTT_PORT   = 8883
MQTT_TOPIC = "reteqfusion/compressor/data"
DB_NAME     = "reteqfusion"
COLLECTION  = "compressor_readings"

# ─────────────────────────────────────────────────────────────────────────
#  Physical validity ranges — same as ESP32 sketch
# ─────────────────────────────────────────────────────────────────────────
VALID_RANGES = {
    "current_a":    (0.0,   30.0),
    "vib_ms2":      (0.0,  100.0),
    "pressure_hpa": (300.0, 1100.0),
    "temp_c":       (-40.0, 85.0),
}

def validate(data: dict) -> bool:
    for field, (lo, hi) in VALID_RANGES.items():
        val = data.get(field)
        if val is None or val == -999:
            return False
        if not (lo <= val <= hi):
            return False
    return True

# ─────────────────────────────────────────────────────────────────────────
#  Uptime tracker — counts 5-minute windows with at least 1 valid message
# ─────────────────────────────────────────────────────────────────────────
uptime_windows  = defaultdict(bool)
total_messages  = 0
invalid_count   = 0

def get_5min_window() -> str:
    now = datetime.now(timezone.utc)
    minute_floor = (now.minute // 5) * 5
    return now.strftime(f"%Y-%m-%d %H:{minute_floor:02d}")

def print_uptime():
    alive  = sum(uptime_windows.values())
    total  = len(uptime_windows)
    pct    = (alive / total * 100) if total > 0 else 0
    print(f"  Uptime    : {pct:.1f}%  ({alive}/{total} 5-min windows with data)")

# ─────────────────────────────────────────────────────────────────────────
#  MongoDB connection
# ─────────────────────────────────────────────────────────────────────────
print("─" * 55)
print("  ReTeqFusion — MQTT → MongoDB Atlas Bridge")
print("─" * 55)
print("Connecting to MongoDB Atlas...")
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    mongo_client.server_info()
    collection   = mongo_client[DB_NAME][COLLECTION]
    print("MongoDB Atlas ✓ connected")
    print(f"  Database   : {DB_NAME}")
    print(f"  Collection : {COLLECTION}")
except ConnectionFailure as e:
    print(f"MongoDB connection failed: {e}")
    print("Check your MONGO_URI and network connection")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────
#  MQTT callbacks
# ─────────────────────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    codes = {
        0: "connected",
        1: "refused — bad protocol",
        2: "refused — client ID rejected",
        3: "refused — server unavailable",
        4: "refused — bad credentials",
        5: "refused — not authorised",
    }
    if rc == 0:
        print(f"\nMQTT broker ✓ {codes[rc]} (TLS port {MQTT_PORT})")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to: {MQTT_TOPIC}")
        print("\nWaiting for data...\n")
    else:
        print(f"MQTT connection failed: {codes.get(rc, f'rc={rc}')}")

def on_message(client, userdata, msg):
    global total_messages, invalid_count

    try:
        raw = json.loads(msg.payload.decode())
        total_messages += 1

        # Validate data quality
        if not validate(raw):
            invalid_count += 1
            quality = ((total_messages - invalid_count) / total_messages * 100)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                  f"⚠ INVALID data rejected — quality: {quality:.1f}%")
            return

        # Mark this 5-minute window as alive
        uptime_windows[get_5min_window()] = True

        # Build enriched MongoDB document
        doc = {
            "node_id":   raw.get("id", "COMP_01"),
            "timestamp": datetime.now(timezone.utc),
            "protocol":  "MQTT over TLS (port 8883)",

            "electrical": {
                "current_a": raw["current_a"],
                "unit":      "A — amperes",
                "note":      "AC mains current drawn by compressor"
            },

            "mechanical": {
                "vibration_ms2": raw["vib_ms2"],
                "unit":          "m/s² — meters per second squared",
                "note":          "Average mechanical vibration over 2s window. Gravity removed. 0 = no vibration."
            },

            "environmental": {
                "pressure_hpa": raw["pressure_hpa"],
                "pressure_unit": "hPa — hectopascals (sea level ≈ 1013 hPa)",
                "temperature_c": raw["temp_c"],
                "temperature_unit": "°C — degrees Celsius"
            },

            "quality": {
                "valid":         True,
                "total_received": total_messages,
                "total_invalid":  invalid_count,
                "quality_pct":   round((total_messages - invalid_count)
                                       / total_messages * 100, 1)
            },

            "raw_payload": raw
        }

        result = collection.insert_one(doc)

        # Console output
        ts = datetime.now().strftime("%H:%M:%S")
        quality_pct = doc["quality"]["quality_pct"]
        print(f"[{ts}] ✓ Saved  _id: {result.inserted_id}")
        print(f"  Current   : {raw['current_a']:.3f} A")
        print(f"  Vibration : {raw['vib_ms2']:.4f} m/s²")
        print(f"  Pressure  : {raw['pressure_hpa']:.2f} hPa")
        print(f"  Temp      : {raw['temp_c']:.2f} °C")
        print(f"  Quality   : {quality_pct:.1f}%  ({total_messages} msgs, "
              f"{invalid_count} rejected)")
        print_uptime()
        print()

    except json.JSONDecodeError:
        print(f"Invalid JSON: {msg.payload}")
    except Exception as e:
        print(f"Error: {e}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"\n[MQTT] Disconnected unexpectedly (rc={rc}) — auto-reconnecting...")

# ─────────────────────────────────────────────────────────────────────────
#  MQTT client — TLS enabled
# ─────────────────────────────────────────────────────────────────────────
mqtt_client = mqtt.Client(client_id="reteqfusion_bridge")
mqtt_client.on_connect    = on_connect
mqtt_client.on_message    = on_message
mqtt_client.on_disconnect = on_disconnect
mqtt_client.tls_set()     # enables TLS — no certificate file needed for HiveMQ public

print(f"\nConnecting to MQTT broker: {MQTT_BROKER}:{MQTT_PORT} (TLS)...")
try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
except Exception as e:
    print(f"MQTT connection error: {e}")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────
#  Graceful shutdown on Ctrl+C
# ─────────────────────────────────────────────────────────────────────────
def shutdown(sig, frame):
    print("\n\nShutting down...")
    print(f"Total messages received : {total_messages}")
    print(f"Total invalid rejected  : {invalid_count}")
    print_uptime()
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    mongo_client.close()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
mqtt_client.loop_forever()