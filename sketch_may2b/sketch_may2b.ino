// ═══════════════════════════════════════════════════════════════════════════
//  ReTeqFusion — Part 1 IoT Node
//  Node ID : COMP_01
//  Board   : ESP32-WROOM-32
//  Sensors : ACS712 (current) · MPU6050 (vibration) · BMP280 (temp/pressure)
//  Protocol: MQTT over TLS (port 8883) via HiveMQ public broker
//  Features: Ring buffer · OTA update · Data validation · Clear units
// ═══════════════════════════════════════════════════════════════════════════

#include <Wire.h>
#include <Adafruit_MPU6050.h>
#include <Adafruit_Sensor.h>
#include <BME280I2C.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <ArduinoOTA.h>


// ═══════════════════════════════════════════════════════════════════════════
const char* WIFI_SSID       = "NAME";       // ← your hotspot name
const char* WIFI_PASSWORD   = "PASSWORD";   // ← your hotspot password
const char* OTA_PASSWORD    = "IPV4";           // ← OTA update password (keep or change)
// ═══════════════════════════════════════════════════════════════════════════
//  These stay as-is — HiveMQ free public TLS broker, no account needed
// ═══════════════════════════════════════════════════════════════════════════
const char* MQTT_BROKER = "broker.emqx.io";
const int   MQTT_PORT   = 8883;
const char* MQTT_TOPIC = "reteqfusion/compressor/data";
const char* CLIENT_ID       = "COMP_01";

// ═══════════════════════════════════════════════════════════════════════════
//  ★ CHANGE IF NEEDED — ACS712 module variant
//    20A module → 0.100
//    30A module → 0.066
//     5A module → 0.185
// ═══════════════════════════════════════════════════════════════════════════
const float ACS_SENSITIVITY = 0.100;

// ─────────────────────────────────────────────────────────────────────────
//  Fixed constants — do not change
// ─────────────────────────────────────────────────────────────────────────
const int   CURRENT_PIN        = 34;
const float ACS_OFFSET         = 2.5;
const float SUPPLY_VOLTAGE     = 3.3;
const int   ADC_MAX            = 4095;
const float GRAVITY            = 9.807;
const int   PUBLISH_INTERVAL   = 2000;   // ms — how often to send to MQTT
const int   VIBRATION_INTERVAL = 100;    // ms — vibration sampling rate

// ─────────────────────────────────────────────────────────────────────────
//  Physical validity ranges for data quality check
//  Values outside these ranges are rejected before publishing
// ─────────────────────────────────────────────────────────────────────────
const float CURRENT_MIN  =   0.0,  CURRENT_MAX  =  30.0;   // Amperes
const float VIB_MIN      =   0.0,  VIB_MAX      = 100.0;   // m/s²
const float PRESSURE_MIN = 300.0,  PRESSURE_MAX = 1100.0;  // hPa
const float TEMP_MIN     = -40.0,  TEMP_MAX     =  85.0;   // °C

// ═══════════════════════════════════════════════════════════════════════════
//  Ring buffer — stores readings when MQTT is offline
//  Increase BUFFER_SIZE if you expect longer disconnections
// ═══════════════════════════════════════════════════════════════════════════
const int BUFFER_SIZE = 20;
char      offlineBuffer[BUFFER_SIZE][256];
int       bufHead  = 0;
int       bufCount = 0;

void bufferPush(const char* json) {
  strncpy(offlineBuffer[bufHead], json, 255);
  bufHead = (bufHead + 1) % BUFFER_SIZE;
  if (bufCount < BUFFER_SIZE) bufCount++;
  Serial.printf("[BUFFER] Stored. %d/%d slots used\n", bufCount, BUFFER_SIZE);
}

void bufferFlush(PubSubClient& client) {
  if (bufCount == 0) return;
  Serial.printf("[BUFFER] Flushing %d stored readings to broker...\n", bufCount);
  int tail    = (bufHead - bufCount + BUFFER_SIZE) % BUFFER_SIZE;
  int flushed = 0;
  while (bufCount > 0) {
    if (client.publish(MQTT_TOPIC, offlineBuffer[tail])) {
      Serial.printf("[BUFFER] Sent: %s\n", offlineBuffer[tail]);
      tail = (tail + 1) % BUFFER_SIZE;
      bufCount--;
      flushed++;
      delay(50);
    } else {
      Serial.println("[BUFFER] Flush failed — broker dropped again");
      break;
    }
  }
  Serial.printf("[BUFFER] Flushed %d readings.\n", flushed);
}

// ─────────────────────────────────────────────────────────────────────────
//  Globals
// ─────────────────────────────────────────────────────────────────────────
Adafruit_MPU6050  mpu;
BME280I2C         bme;
WiFiClientSecure  espClient;        // secure client for TLS
PubSubClient      mqtt(espClient);

bool bmeOK = false;
bool mpuOK = false;

float         sumVibration  = 0.0;
int           vibSamples    = 0;
unsigned long lastVibSample = 0;
unsigned long lastPublish   = 0;

// ─────────────────────────────────────────────────────────────────────────
//  Data validation
// ─────────────────────────────────────────────────────────────────────────
bool isDataValid(float current_a, float vib, float pressure, float temp) {
  if (isnan(current_a) || current_a < CURRENT_MIN  || current_a > CURRENT_MAX)  return false;
  if (isnan(vib)       || vib       < VIB_MIN       || vib       > VIB_MAX)      return false;
  if (isnan(pressure)  || pressure  < PRESSURE_MIN  || pressure  > PRESSURE_MAX) return false;
  if (isnan(temp)      || temp      < TEMP_MIN       || temp      > TEMP_MAX)     return false;
  return true;
}

// ─────────────────────────────────────────────────────────────────────────
//  Wi-Fi
// ─────────────────────────────────────────────────────────────────────────
void connectWiFi() {
  Serial.printf("\nConnecting to Wi-Fi: %s", WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
    if (++attempts > 40) {
      Serial.println("\n[WIFI] Timeout — will retry in loop");
      return;
    }
  }
  Serial.printf("\n[WIFI] Connected. IP: %s\n", WiFi.localIP().toString().c_str());
}

// ─────────────────────────────────────────────────────────────────────────
//  MQTT reconnect — non-blocking single attempt
// ─────────────────────────────────────────────────────────────────────────
bool reconnectMQTT() {
  if (WiFi.status() != WL_CONNECTED) return false;
  Serial.printf("[MQTT] Connecting to %s:%d ...", MQTT_BROKER, MQTT_PORT);
  if (mqtt.connect(CLIENT_ID)) {
    Serial.println(" connected! (TLS)");
    bufferFlush(mqtt);
    return true;
  }
  Serial.printf(" failed (rc=%d) — retrying in 5s\n", mqtt.state());
  delay(5000);
  return false;
}

// ═══════════════════════════════════════════════════════════════════════════
//  SETUP
// ═══════════════════════════════════════════════════════════════════════════
void setup() {
  Serial.begin(115200);
  delay(500);
  Wire.begin(21, 22);

  // ── Sensor init ──────────────────────────────────────────────────────────
  Serial.println("\n╔══════════════════════════╗");
  Serial.println("║   SENSOR SELF-TEST       ║");
  Serial.println("╚══════════════════════════╝");

  if (bme.begin()) {
    Serial.println("[BMP280]  ✓ found");
    bmeOK = true;
  } else {
    Serial.println("[BMP280]  ✗ NOT FOUND — check SDA→GPIO21, SCL→GPIO22, CSB→3.3V");
  }

  if (mpu.begin()) {
    Serial.println("[MPU6050] ✓ found");
    mpuOK = true;
    mpu.setAccelerometerRange(MPU6050_RANGE_8_G);
    mpu.setGyroRange(MPU6050_RANGE_500_DEG);
    mpu.setFilterBandwidth(MPU6050_BAND_21_HZ);
  } else {
    Serial.println("[MPU6050] ✗ NOT FOUND — check SDA→GPIO21, SCL→GPIO22, AD0→GND");
  }

  int rawTest = analogRead(CURRENT_PIN);
  if (rawTest > 0 && rawTest < ADC_MAX) {
    Serial.printf("[ACS712]  ✓ ADC reading: %d\n", rawTest);
  } else {
    Serial.printf("[ACS712]  ⚠ ADC reading: %d — check OUT→GPIO34\n", rawTest);
  }

  Serial.println("══════════════════════════\n");

  // ── Wi-Fi ────────────────────────────────────────────────────────────────
  connectWiFi();

  // ── TLS — accept broker certificate without CA verification ──────────────
  espClient.setInsecure();

  // ── MQTT ─────────────────────────────────────────────────────────────────
  mqtt.setServer(MQTT_BROKER, MQTT_PORT);
  mqtt.setBufferSize(512);

  // ── OTA update setup ─────────────────────────────────────────────────────
  ArduinoOTA.setHostname(CLIENT_ID);
  ArduinoOTA.setPassword(OTA_PASSWORD);
  ArduinoOTA.onStart([]() {
    Serial.println("[OTA] Update starting...");
  });
  ArduinoOTA.onEnd([]() {
    Serial.println("[OTA] Update complete!");
  });
  ArduinoOTA.onProgress([](unsigned int progress, unsigned int total) {
    Serial.printf("[OTA] Progress: %u%%\r", (progress / (total / 100)));
  });
  ArduinoOTA.onError([](ota_error_t error) {
    Serial.printf("[OTA] Error[%u]\n", error);
  });
  ArduinoOTA.begin();
  Serial.printf("[OTA] Ready — hostname: %s  password: %s\n", CLIENT_ID, OTA_PASSWORD);
}

// ═══════════════════════════════════════════════════════════════════════════
//  LOOP
// ═══════════════════════════════════════════════════════════════════════════
void loop() {
  // Always handle OTA first
  ArduinoOTA.handle();

  // Wi-Fi watchdog
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[WIFI] Connection lost — reconnecting...");
    connectWiFi();
  }

  // MQTT watchdog
  if (!mqtt.connected()) reconnectMQTT();
  mqtt.loop();

  unsigned long now = millis();

  // ── Vibration sampling — every 100ms ─────────────────────────────────────
  if (mpuOK && (now - lastVibSample >= VIBRATION_INTERVAL)) {
    lastVibSample = now;
    sensors_event_t a, g, temp;
    mpu.getEvent(&a, &g, &temp);
    float total         = sqrt(sq(a.acceleration.x) +
                               sq(a.acceleration.y) +
                               sq(a.acceleration.z));
    float pureVibration = abs(total - GRAVITY);  // subtract gravity baseline
    sumVibration += pureVibration;
    vibSamples++;
  }

  // ── Publish every 2 seconds ───────────────────────────────────────────────
  if (now - lastPublish >= PUBLISH_INTERVAL) {
    lastPublish = now;

    // 1. Current — ACS712 → Amperes
    int   rawADC    = analogRead(CURRENT_PIN);
    float voltage   = (rawADC / (float)ADC_MAX) * SUPPLY_VOLTAGE;
    float current_a = abs(voltage - ACS_OFFSET) / ACS_SENSITIVITY;

    // 2. Vibration — average of last 20 samples (100ms × 20 = 2s window)
    float vib_avg = (vibSamples > 0) ? sumVibration / vibSamples : 0.0;
    sumVibration  = 0.0;
    vibSamples    = 0;

    // 3. Pressure & temperature — BMP280
    float pressure    = 0.0;
    float ambientTemp = 0.0;
    if (bmeOK) {
      BME280::TempUnit tempUnit(BME280::TempUnit_Celsius);
      BME280::PresUnit presUnit(BME280::PresUnit_hPa);
      float hum;
      bme.read(pressure, ambientTemp, hum, tempUnit, presUnit);
    }

    // 4. Validate before publishing — reject nulls and out-of-range values
    if (!isDataValid(current_a, vib_avg, pressure, ambientTemp)) {
      Serial.println("[VALIDATION] ⚠ Out-of-range value detected — skipping publish");
      return;
    }

    // 5. Build JSON with clear field names
    StaticJsonDocument<300> doc;
    doc["id"]           = CLIENT_ID;
    doc["current_a"]    = round(current_a * 1000.0) / 1000.0;  // 3 decimal places
    doc["vib_ms2"]      = round(vib_avg   * 10000.0) / 10000.0; // 4 decimal places
    doc["pressure_hpa"] = round(pressure  * 100.0)   / 100.0;
    doc["temp_c"]       = round(ambientTemp * 100.0)  / 100.0;

    char jsonOut[300];
    serializeJson(doc, jsonOut);

    // 6. Print to Serial Monitor with full units
    Serial.println("┌─────────────────────────────────────────┐");
    Serial.printf( "│ Current     : %7.3f A  (amperes)       │\n", current_a);
    Serial.printf( "│ Vibration   : %7.4f m/s² (mech. only) │\n", vib_avg);
    Serial.printf( "│ Pressure    : %7.2f hPa               │\n", pressure);
    Serial.printf( "│ Temperature : %7.2f °C                │\n", ambientTemp);
    Serial.printf( "│ Payload     : %-26s│\n", jsonOut);
    Serial.println("└─────────────────────────────────────────┘");

    // 7. Publish or buffer if offline
    if (mqtt.connected()) {
      if (mqtt.publish(MQTT_TOPIC, jsonOut)) {
        Serial.println("[MQTT] ✓ Published via TLS");
      } else {
        Serial.println("[MQTT] ✗ Publish failed — buffering");
        bufferPush(jsonOut);
      }
    } else {
      Serial.println("[MQTT] Offline — buffering reading");
      bufferPush(jsonOut);
    }

    Serial.println();
  }
}
