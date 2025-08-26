/*
  PanelPro — ESP32-S3 Firmware (LUT mapping + AP+STA + WS logs)
  - AsyncWebServer + LittleFS
  - Universal LUT: canvasIndex -> stripIndex for any tiling/orientation
  - Strict /api/frame (octet-stream), pixel fallback, test routes
  - AP always on; optional STA; logs via Serial + WebSocket
*/

#include <Arduino.h>
#include <WiFi.h>
#include <esp_wifi.h>
#include <ESPmDNS.h>

#include <LittleFS.h>
#include <AsyncTCP.h>
#include <ESPAsyncWebServer.h>
#include <ArduinoJson.h>
#include <Adafruit_NeoPixel.h>
#include "mbedtls/base64.h"

#include <vector>
#include <deque>

#include "esp_task_wdt.h"

// ---------- Build / pins ----------
#ifndef LED_BUILTIN
#define LED_BUILTIN 2
#endif
#ifndef LED_PIN
#define LED_PIN 5  // your data pin
#endif

#define FW_NAME "PanelPro"
#define FW_VERSION "0.9.3"

// ---- Mapping enums (must be above any use) ----
enum Corner : uint8_t { CORNER_TL = 0,
                        CORNER_TR = 1,
                        CORNER_BL = 2,
                        CORNER_BR = 3 };
enum Scan : uint8_t { SCAN_ROWS = 0,
                      SCAN_COLS = 1 };
enum Path : uint8_t { PATH_PROGRESSIVE = 0,
                      PATH_SERPENTINE = 1 };

// ---------- Logging ----------
AsyncWebServer server(80);
AsyncWebSocket ws("/ws/log");
std::deque<String> logRing;  // last N lines for new WS clients
const size_t LOG_RING_MAX = 200;

static void wsBroadcast(const String& s) {
  ws.textAll(s);
}
static void logLine(const char* level, const char* fmt, ...) {
  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  String line = "[" + String(level) + "] " + String(buf);
  Serial.println(line);
  logRing.push_back(line);
  if (logRing.size() > LOG_RING_MAX) logRing.pop_front();
  wsBroadcast(line);
}
#define LOGI(...) logLine("I", __VA_ARGS__)
#define LOGW(...) logLine("W", __VA_ARGS__)
#define LOGE(...) logLine("E", __VA_ARGS__)

// panel & mapping config (persisted)
struct PanelConfig {
  uint16_t tilesX = 2, tilesY = 2;
  uint16_t tileW = 8, tileH = 8;

  // chain of tiles (large-scale walk)
  Corner chainStart = CORNER_TL;
  Scan chainScan = SCAN_ROWS;
  Path chainPath = PATH_PROGRESSIVE;

  // allow explicit order override (length = tilesX*tilesY). -1 = unused
  std::vector<int16_t> chainOrder;  // index = walk position, value = tileIndex

  // inside each 8x8 tile
  Corner inStart = CORNER_BR;  // common for many 8x8 panels
  Scan inScan = SCAN_COLS;     // vertical first
  Path inPath = PATH_PROGRESSIVE;

  // per-tile overrides (same length as numTiles)
  std::vector<uint8_t> rot;    // 0,1,2,3 -> *90deg
  std::vector<uint8_t> flipH;  // 0/1
  std::vector<uint8_t> flipV;  // 0/1
};

PanelConfig cfg;

static uint16_t fullW() {
  return cfg.tilesX * cfg.tileW;
}
static uint16_t fullH() {
  return cfg.tilesY * cfg.tileH;
}
static uint32_t ledCount() {
  return (uint32_t)fullW() * (uint32_t)fullH();
}

// LUT: canvas index -> strip index
std::vector<uint32_t> lut;

// LED strip
Adafruit_NeoPixel* strip = nullptr;
uint8_t brightness = 64;
uint32_t powerLimitMA = 2000;  // not used yet (future power limiting)

// payload accumulator for /api/frame
std::vector<uint8_t> frameBody;

// payload accumulator for /api/matrix/config (raw JSON)
static String cfgBody;

// ---------- Helpers ----------
static inline uint32_t canvasIndex(uint16_t x, uint16_t y) {
  return (uint32_t)y * (uint32_t)fullW() + x;
}
static inline uint32_t clampu(uint32_t v, uint32_t lo, uint32_t hi) {
  return v < lo ? lo : (v > hi ? hi : v);
}

static void savePanelJson() {
  DynamicJsonDocument doc(8192);
  doc["tilesX"] = cfg.tilesX;
  doc["tilesY"] = cfg.tilesY;
  doc["tileW"] = cfg.tileW;
  doc["tileH"] = cfg.tileH;

  doc["chainStart"] = (int)cfg.chainStart;
  doc["chainScan"] = (int)cfg.chainScan;
  doc["chainPath"] = (int)cfg.chainPath;

  if (!cfg.chainOrder.empty()) {
    auto a = doc.createNestedArray("chainOrder");
    for (auto v : cfg.chainOrder) a.add(v);
  }

  doc["inStart"] = (int)cfg.inStart;
  doc["inScan"] = (int)cfg.inScan;
  doc["inPath"] = (int)cfg.inPath;

  if (!cfg.rot.empty()) {
    auto a = doc.createNestedArray("rot");
    for (auto v : cfg.rot) a.add(v);
    a = doc.createNestedArray("flipH");
    for (auto v : cfg.flipH) a.add(v);
    a = doc.createNestedArray("flipV");
    for (auto v : cfg.flipV) a.add(v);
  }

  File f = LittleFS.open("/panel.json", "w");
  if (!f) {
    LOGE("save panel.json: open failed");
    return;
  }
  if (serializeJson(doc, f) == 0) LOGE("save panel.json: serialize failed");
  f.close();
  LOGI("Saved /panel.json");
}

static bool loadPanelJson() {
  File f = LittleFS.open("/panel.json", "r");
  if (!f) return false;
  DynamicJsonDocument doc(8192);
  auto err = deserializeJson(doc, f);
  f.close();
  if (err) {
    LOGW("panel.json parse: %s", err.c_str());
    return false;
  }

  cfg.tilesX = doc["tilesX"] | cfg.tilesX;
  cfg.tilesY = doc["tilesY"] | cfg.tilesY;
  cfg.tileW = doc["tileW"] | cfg.tileW;
  cfg.tileH = doc["tileH"] | cfg.tileH;

  cfg.chainStart = (Corner)(int)(doc["chainStart"] | cfg.chainStart);
  cfg.chainScan = (Scan)(int)(doc["chainScan"] | cfg.chainScan);
  cfg.chainPath = (Path)(int)(doc["chainPath"] | cfg.chainPath);

  cfg.chainOrder.clear();
  if (doc.containsKey("chainOrder")) {
    for (JsonVariant v : doc["chainOrder"].as<JsonArray>()) cfg.chainOrder.push_back((int16_t)v.as<int>());
  }

  cfg.inStart = (Corner)(int)(doc["inStart"] | cfg.inStart);
  cfg.inScan = (Scan)(int)(doc["inScan"] | cfg.inScan);
  cfg.inPath = (Path)(int)(doc["inPath"] | cfg.inPath);

  const uint16_t tiles = cfg.tilesX * cfg.tilesY;
  cfg.rot.assign(tiles, 0);
  cfg.flipH.assign(tiles, 0);
  cfg.flipV.assign(tiles, 0);

  if (doc.containsKey("rot")) {
    uint16_t i = 0;
    for (JsonVariant v : doc["rot"].as<JsonArray>())
      if (i < tiles) cfg.rot[i++] = (uint8_t)v.as<int>();
  }
  if (doc.containsKey("flipH")) {
    uint16_t i = 0;
    for (JsonVariant v : doc["flipH"].as<JsonArray>())
      if (i < tiles) cfg.flipH[i++] = (uint8_t)v.as<int>();
  }
  if (doc.containsKey("flipV")) {
    uint16_t i = 0;
    for (JsonVariant v : doc["flipV"].as<JsonArray>())
      if (i < tiles) cfg.flipV[i++] = (uint8_t)v.as<int>();
  }

  LOGI("Loaded /panel.json");
  return true;
}

// ---------- Mapping math ----------
static inline void applyRotFlip(uint16_t& x, uint16_t& y, uint16_t w, uint16_t h,
                                uint8_t rot90, bool fh, bool fv) {
  rot90 %= 4;
  uint16_t nx = x, ny = y;
  if (rot90 == 1) {
    nx = y;
    ny = (w - 1) - x;
    std::swap(w, h);
  } else if (rot90 == 2) {
    nx = (w - 1) - x;
    ny = (h - 1) - y;
  } else if (rot90 == 3) {
    nx = (h - 1) - y;
    ny = x;
    std::swap(w, h);
  }
  // flips in the rotated frame
  if (fh) nx = (w - 1) - nx;
  if (fv) ny = (h - 1) - ny;
  x = nx;
  y = ny;
}

static void makeInsideOrder(std::vector<uint16_t>& xs, std::vector<uint16_t>& ys,
                            uint16_t w, uint16_t h, Corner start, Scan scan, Path path) {
  xs.clear();
  ys.clear();
  if (scan == SCAN_ROWS) {
    for (uint16_t ry = 0; ry < h; ++ry) {
      bool rev = (path == PATH_SERPENTINE && (ry & 1));
      if (!rev) {
        for (uint16_t rx = 0; rx < w; ++rx) {
          xs.push_back(rx);
          ys.push_back(ry);
        }
      } else {
        for (int rx = w - 1; rx >= 0; --rx) {
          xs.push_back(rx);
          ys.push_back(ry);
        }
      }
    }
  } else {  // SCAN_COLS
    for (uint16_t cx = 0; cx < w; ++cx) {
      bool rev = (path == PATH_SERPENTINE && (cx & 1));
      if (!rev) {
        for (uint16_t cy = 0; cy < h; ++cy) {
          xs.push_back(cx);
          ys.push_back(cy);
        }
      } else {
        for (int cy = h - 1; cy >= 0; --cy) {
          xs.push_back(cx);
          ys.push_back(cy);
        }
      }
    }
  }
  // convert TL origin to requested corner by flips
  for (size_t i = 0; i < xs.size(); ++i) {
    uint16_t x = xs[i], y = ys[i];
    bool fh = false, fv = false;
    if (start == CORNER_TR || start == CORNER_BR) fh = true;
    if (start == CORNER_BL || start == CORNER_BR) fv = true;
    if (fh) x = (w - 1) - x;
    if (fv) y = (h - 1) - y;
    xs[i] = x;
    ys[i] = y;
  }
}

static int tileIndexFromXY(uint16_t tx, uint16_t ty) {
  return (int)(ty * cfg.tilesX + tx);
}

static void chainOrderParametric(std::vector<int16_t>& order) {
  order.clear();
  order.reserve(cfg.tilesX * cfg.tilesY);

  std::vector<std::pair<uint16_t, uint16_t>> cells;
  if (cfg.chainScan == SCAN_ROWS) {
    for (uint16_t ty = 0; ty < cfg.tilesY; ++ty) {
      bool rev = (cfg.chainPath == PATH_SERPENTINE && (ty & 1));
      if (!rev)
        for (uint16_t tx = 0; tx < cfg.tilesX; ++tx) cells.push_back({ tx, ty });
      else
        for (int tx = cfg.tilesX - 1; tx >= 0; --tx) cells.push_back({ (uint16_t)tx, ty });
    }
  } else {  // SCAN_COLS
    for (uint16_t tx = 0; tx < cfg.tilesX; ++tx) {
      bool rev = (cfg.chainPath == PATH_SERPENTINE && (tx & 1));
      if (!rev)
        for (uint16_t ty = 0; ty < cfg.tilesY; ++ty) cells.push_back({ tx, ty });
      else
        for (int ty = cfg.tilesY - 1; ty >= 0; --ty) cells.push_back({ tx, (uint16_t)ty });
    }
  }
  // adapt to requested start corner by flipping grid
  for (auto& p : cells) {
    uint16_t tx = p.first, ty = p.second;
    bool fh = false, fv = false;
    if (cfg.chainStart == CORNER_TR || cfg.chainStart == CORNER_BR) fh = true;
    if (cfg.chainStart == CORNER_BL || cfg.chainStart == CORNER_BR) fv = true;
    if (fh) tx = (cfg.tilesX - 1) - tx;
    if (fv) ty = (cfg.tilesY - 1) - ty;
    order.push_back(tileIndexFromXY(tx, ty));
  }
}

static void rebuildLUT() {
  const uint16_t W = fullW(), H = fullH();
  const uint32_t N = (uint32_t)W * (uint32_t)H;
  lut.assign(N, 0xFFFFFFFF);

  // make chain order (tile walk)
  std::vector<int16_t> walk;
  if (!cfg.chainOrder.empty() && cfg.chainOrder.size() == (size_t)(cfg.tilesX * cfg.tilesY)) {
    walk = cfg.chainOrder;
  } else {
    chainOrderParametric(walk);
  }

  // inside-tile order (defaults)
  std::vector<uint16_t> lx, ly;
  makeInsideOrder(lx, ly, cfg.tileW, cfg.tileH, cfg.inStart, cfg.inScan, cfg.inPath);

  const uint16_t tiles = cfg.tilesX * cfg.tilesY;
  if (cfg.rot.size() != tiles) cfg.rot.assign(tiles, 0);
  if (cfg.flipH.size() != tiles) cfg.flipH.assign(tiles, 0);
  if (cfg.flipV.size() != tiles) cfg.flipV.assign(tiles, 0);

  uint32_t stripIdx = 0;
  for (uint16_t step = 0; step < walk.size(); ++step) {
    int t = walk[step];
    if (t < 0 || t >= tiles) {
      LOGW("walk[%u]=%d out of range", step, t);
      continue;
    }
    uint16_t ty = t / cfg.tilesX;
    uint16_t tx = t % cfg.tilesX;
    uint16_t ox = tx * cfg.tileW;
    uint16_t oy = ty * cfg.tileH;

    for (size_t k = 0; k < lx.size(); ++k) {
      uint16_t x = lx[k], y = ly[k];
      applyRotFlip(x, y, cfg.tileW, cfg.tileH, cfg.rot[t], cfg.flipH[t], cfg.flipV[t]);
      uint16_t gx = ox + x;
      uint16_t gy = oy + y;
      if (gx < W && gy < H && stripIdx < N) {
        lut[canvasIndex(gx, gy)] = stripIdx++;
      }
    }
  }

  // sanity
  uint32_t holesCanvas = 0;
  for (auto v : lut)
    if (v == 0xFFFFFFFF) holesCanvas++;
  if (holesCanvas) LOGW("LUT built with %u canvas holes (check config!)", (unsigned)holesCanvas);
  else LOGI("LUT built: %ux%u (%u leds)", W, H, (unsigned)N);
}

// ---------- LEDs ----------
static void ensureStrip() {
  const uint32_t N = ledCount();
  if (!strip) {
    strip = new Adafruit_NeoPixel(N, LED_PIN, NEO_GRB + NEO_KHZ800);  // change color order here if needed
    strip->begin();
    strip->setBrightness(brightness);
    strip->clear();
    strip->show();
  } else if (strip->numPixels() != N) {
    strip->updateLength(N);
    strip->setBrightness(brightness);
    strip->clear();
    strip->show();
  }
}

// ---------- WiFi ----------
String staSSID, staPASS;

static void startAP() {
  WiFi.mode(WIFI_AP_STA);
  bool ok = WiFi.softAP("PanelPro-Setup");
  if (ok) LOGI("AP started: SSID=PanelPro-Setup IP=%s", WiFi.softAPIP().toString().c_str());
  else LOGE("AP start failed");
}

static void trySTA() {
  if (staSSID.length() == 0) return;
  LOGI("STA: connecting to SSID='%s'...", staSSID.c_str());
  WiFi.begin(staSSID.c_str(), staPASS.c_str());
}

static void loadWiFiJson() {
  File f = LittleFS.open("/wifi.json", "r");
  if (!f) return;
  DynamicJsonDocument doc(512);
  if (!deserializeJson(doc, f)) {
    staSSID = doc["ssid"] | "";
    staPASS = doc["pass"] | "";
  }
  f.close();
  if (staSSID.length()) LOGI("WiFi creds loaded (ssid='%s')", staSSID.c_str());
}

static void saveWiFiJson() {
  DynamicJsonDocument doc(512);
  doc["ssid"] = staSSID;
  doc["pass"] = staPASS;
  File f = LittleFS.open("/wifi.json", "w");
  if (!f) {
    LOGE("wifi.json open failed");
    return;
  }
  serializeJson(doc, f);
  f.close();
  LOGI("WiFi creds saved");
}

// ---------- HTTP helpers ----------
static void sendJSON(AsyncWebServerRequest* req, DynamicJsonDocument& doc, int code = 200) {
  String out;
  serializeJson(doc, out);
  AsyncWebServerResponse* res = req->beginResponse(code, F("application/json"), out);
  res->addHeader(F("Access-Control-Allow-Origin"), F("*"));
  res->addHeader(F("Access-Control-Allow-Methods"), F("GET,POST,DELETE,OPTIONS"));
  res->addHeader(F("Access-Control-Allow-Headers"), F("*"));
  req->send(res);
}
static void sendError(AsyncWebServerRequest* req, int code, const String& msg) {
  DynamicJsonDocument d(256);
  d["error"] = msg;
  sendJSON(req, d, code);
}

// ---------- Routes ----------
static void onBody(AsyncWebServerRequest* request, uint8_t* data, size_t len, size_t index, size_t total) {
  if (request->url() == "/api/frame") {
    if (index == 0) {
      frameBody.clear();
      frameBody.reserve(total ? total : len);
      LOGI("frame body start total=%u", (unsigned)total);
    }
    frameBody.insert(frameBody.end(), data, data + len);
  }
}
static bool decodeBase64To(std::vector<uint8_t>& dst, const uint8_t* in, size_t inLen) {
  size_t need = 0;
  int rc = mbedtls_base64_decode(nullptr, 0, &need, in, inLen);
  if (rc != MBEDTLS_ERR_BASE64_BUFFER_TOO_SMALL) return false;
  dst.resize(need);
  rc = mbedtls_base64_decode(dst.data(), need, &need, in, inLen);
  if (rc != 0) return false;
  dst.resize(need);
  return true;
}

// Build a compact JSON report of LUT health
static void buildLUTReport(DynamicJsonDocument& d) {
  const uint32_t N = ledCount();
  d["N"] = N;

  uint32_t canvas_holes = 0, oob = 0;
  std::vector<uint16_t> counts(N, 0);

  for (uint32_t i = 0; i < lut.size(); ++i) {
    uint32_t si = lut[i];
    if (si == 0xFFFFFFFF) {
      canvas_holes++;
      continue;
    }
    if (si >= N) {
      oob++;
      continue;
    }
    counts[si]++;
  }

  uint32_t dups = 0, holes = 0;
  for (uint32_t si = 0; si < N; ++si) {
    if (counts[si] == 0) holes++;
    else if (counts[si] > 1) dups++;
  }

  d["canvas_holes"] = canvas_holes;  // canvas indices not mapped
  d["oob"] = oob;                    // lut entries >= N
  d["dups"] = dups;                  // physical indices with count > 1
  d["holes"] = holes;                // physical indices with count == 0

  JsonArray dup_list = d.createNestedArray("dup_si");
  JsonArray hole_list = d.createNestedArray("hole_si");
  uint16_t cap = 10;
  for (uint32_t si = 0; si < N && cap; ++si)
    if (counts[si] > 1) {
      dup_list.add(si);
      cap--;
    }
  cap = 10;
  for (uint32_t si = 0; si < N && cap; ++si)
    if (counts[si] == 0) {
      hole_list.add(si);
      cap--;
    }

  // also include LUT head/tail preview (handy)
  JsonArray head = d.createNestedArray("head");
  for (uint32_t i = 0; i < min<uint32_t>(10, lut.size()); ++i) head.add(lut[i]);
  JsonArray tail = d.createNestedArray("tail");
  uint32_t start = (lut.size() > 10) ? (lut.size() - 10) : 0;
  for (uint32_t i = start; i < lut.size(); ++i) tail.add(lut[i]);
}

static void setupRoutes() {
  DefaultHeaders::Instance().addHeader("Access-Control-Allow-Origin", "*");
  DefaultHeaders::Instance().addHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
  DefaultHeaders::Instance().addHeader("Access-Control-Allow-Headers", "*");

  server.on("/test", HTTP_GET, [](AsyncWebServerRequest* r) {
    r->send(200, "text/plain", "OK");
  });

  server.on("/api/status", HTTP_GET, [](AsyncWebServerRequest* req) {
    DynamicJsonDocument d(2048);
    d["firmware"] = String(FW_NAME) + " " + FW_VERSION;
    auto jChip = d.createNestedObject("chip");
    jChip["model"] = ESP.getChipModel();
    jChip["cores"] = ESP.getChipCores();
    jChip["rev"] = ESP.getChipRevision();
    jChip["cpu_mhz"] = ESP.getCpuFreqMHz();
    jChip["sdk"] = ESP.getSdkVersion();

    auto jWiFi = d.createNestedObject("wifi");
    jWiFi["ap_ip"] = WiFi.softAPIP().toString();
    jWiFi["mode"] = (int)WiFi.getMode();
    jWiFi["sta_ok"] = (WiFi.status() == WL_CONNECTED);
    jWiFi["sta_ssid"] = WiFi.SSID();
    jWiFi["sta_ip"] = WiFi.localIP().toString();

    auto jM = d.createNestedObject("matrix");
    jM["W"] = fullW();
    jM["H"] = fullH();
    jM["leds"] = ledCount();

    auto jFS = d.createNestedObject("fs");
    jFS["total"] = LittleFS.totalBytes();
    jFS["used"] = LittleFS.usedBytes();

    sendJSON(req, d, 200);
  });

  // ---- Matrix config ----
  server.on("/api/matrix/config", HTTP_OPTIONS, [](AsyncWebServerRequest* req) {
    auto* res = req->beginResponse(204);
    res->addHeader("Access-Control-Allow-Origin", "*");
    res->addHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
    res->addHeader("Access-Control-Allow-Headers", "*");
    req->send(res);
  });

  server.on("/api/matrix/config", HTTP_GET, [](AsyncWebServerRequest* req) {
    File f = LittleFS.open("/panel.json", "r");
    if (f) {
      String body = f.readString();
      f.close();
      auto* res = req->beginResponse(200, "application/json", body);
      res->addHeader("Access-Control-Allow-Origin", "*");
      req->send(res);
      return;
    }
    DynamicJsonDocument doc(4096);
    doc["tilesX"] = cfg.tilesX;
    doc["tilesY"] = cfg.tilesY;
    doc["tileW"] = cfg.tileW;
    doc["tileH"] = cfg.tileH;
    doc["chainStart"] = (int)cfg.chainStart;
    doc["chainScan"] = (int)cfg.chainScan;
    doc["chainPath"] = (int)cfg.chainPath;
    doc["inStart"] = (int)cfg.inStart;
    doc["inScan"] = (int)cfg.inScan;
    doc["inPath"] = (int)cfg.inPath;

    JsonArray order = doc.createNestedArray("chainOrder");
    for (auto v : cfg.chainOrder) order.add((int)v);

    JsonArray r = doc.createNestedArray("rot");
    JsonArray fh = doc.createNestedArray("flipH");
    JsonArray fv = doc.createNestedArray("flipV");
    for (uint16_t i = 0; i < cfg.rot.size(); ++i) {
      r.add(cfg.rot[i]);
      fh.add(cfg.flipH[i]);
      fv.add(cfg.flipV[i]);
    }

    sendJSON(req, doc, 200);
  });

  server.on(
    "/api/matrix/config",
    HTTP_POST,
    [](AsyncWebServerRequest* req) {
      if (cfgBody.isEmpty() && req->hasParam("plain", true)) {
        cfgBody = req->getParam("plain", true)->value();
      }
      if (cfgBody.isEmpty()) {
        sendError(req, 400, "no json");
        return;
      }

      DynamicJsonDocument doc(8192);
      DeserializationError err = deserializeJson(doc, cfgBody);
      cfgBody = "";
      if (err) {
        sendError(req, 400, String("json parse: ") + err.c_str());
        return;
      }

      cfg.tilesX = doc["tilesX"] | cfg.tilesX;
      cfg.tilesY = doc["tilesY"] | cfg.tilesY;
      cfg.tileW = doc["tileW"] | cfg.tileW;
      cfg.tileH = doc["tileH"] | cfg.tileH;

      cfg.chainStart = (Corner)(int)(doc["chainStart"] | cfg.chainStart);
      cfg.chainScan = (Scan)(int)(doc["chainScan"] | cfg.chainScan);
      cfg.chainPath = (Path)(int)(doc["chainPath"] | cfg.chainPath);

      cfg.inStart = (Corner)(int)(doc["inStart"] | cfg.inStart);
      cfg.inScan = (Scan)(int)(doc["inScan"] | cfg.inScan);
      cfg.inPath = (Path)(int)(doc["inPath"] | cfg.inPath);

      const uint16_t tiles = cfg.tilesX * cfg.tilesY;

      cfg.chainOrder.clear();
      if (doc.containsKey("chainOrder")) {
        for (JsonVariant v : doc["chainOrder"].as<JsonArray>()) {
          cfg.chainOrder.push_back((int16_t)v.as<int>());
        }
      }

      cfg.rot.assign(tiles, 0);
      cfg.flipH.assign(tiles, 0);
      cfg.flipV.assign(tiles, 0);
      if (doc.containsKey("rot")) {
        uint16_t i = 0;
        for (JsonVariant v : doc["rot"].as<JsonArray>())
          if (i < tiles) cfg.rot[i++] = (uint8_t)v.as<int>();
      }
      if (doc.containsKey("flipH")) {
        uint16_t i = 0;
        for (JsonVariant v : doc["flipH"].as<JsonArray>())
          if (i < tiles) cfg.flipH[i++] = (uint8_t)v.as<int>();
      }
      if (doc.containsKey("flipV")) {
        uint16_t i = 0;
        for (JsonVariant v : doc["flipV"].as<JsonArray>())
          if (i < tiles) cfg.flipV[i++] = (uint8_t)v.as<int>();
      }

      savePanelJson();
      ensureStrip();
      rebuildLUT();

      DynamicJsonDocument ok(128);
      ok["ok"] = true;
      ok["W"] = fullW();
      ok["H"] = fullH();
      sendJSON(req, ok, 200);
    },
    /* onUpload */ nullptr,
    /* onBody   */ [](AsyncWebServerRequest* req, uint8_t* data, size_t len, size_t index, size_t total) {
      if (index == 0) cfgBody = "";
      cfgBody.reserve(total ? total : (cfgBody.length() + len));
      cfgBody.concat((const char*)data, len);
    });

  // Inspect LUT quickly (preview)
  server.on("/api/matrix/lut", HTTP_GET, [](AsyncWebServerRequest* req) {
    DynamicJsonDocument d(1024);
    d["size"] = (uint32_t)lut.size();
    size_t headN = (lut.size() < 10) ? lut.size() : 10;
    JsonArray head = d.createNestedArray("head");
    for (size_t i = 0; i < headN; ++i) head.add(lut[i]);
    JsonArray tail = d.createNestedArray("tail");
    size_t start = (lut.size() > 10) ? (lut.size() - 10) : 0;
    for (size_t i = start; i < lut.size(); ++i) tail.add(lut[i]);
    sendJSON(req, d, 200);
  });

  // **NEW**: Full LUT verification (holes/dups/oob + samples)
  server.on("/api/matrix/verify", HTTP_GET, [](AsyncWebServerRequest* req) {
    DynamicJsonDocument d(4096);
    buildLUTReport(d);
    sendJSON(req, d, 200);
  });

  // ---- Frame ----
  server.on("/api/frame", HTTP_OPTIONS, [](AsyncWebServerRequest* r) {
    auto* res = r->beginResponse(204);
    res->addHeader("Access-Control-Allow-Origin", "*");
    res->addHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
    res->addHeader("Access-Control-Allow-Headers", "*");
    r->send(res);
  });

  server.on(
    "/api/frame", HTTP_POST, [](AsyncWebServerRequest* req) {
      const uint32_t expected = ledCount() * 3;
      std::vector<uint8_t> payload;

      String ct = req->contentType();
      if (ct.indexOf("application/octet-stream") >= 0) {
        payload.swap(frameBody);
        frameBody.clear();
      } else if (ct.indexOf("application/x-www-form-urlencoded") >= 0) {
        String body;
        body.reserve(frameBody.size() + 1);
        for (auto c : frameBody) body += (char)c;
        frameBody.clear();
        int p = body.indexOf("data=");
        if (p < 0) return sendError(req, 400, "no data field");
        String b64 = body.substring(p + 5);
        int amp = b64.indexOf('&');
        if (amp >= 0) b64 = b64.substring(0, amp);
        if (!decodeBase64To(payload, (const uint8_t*)b64.c_str(), b64.length()))
          return sendError(req, 400, "base64 decode failed");
      } else {
        return sendError(req, 415, "unsupported content-type");
      }

      if (payload.size() != expected) {
        DynamicJsonDocument d(256);
        d["error"] = "bad length";
        d["got"] = (uint32_t)payload.size();
        d["expected"] = expected;
        return sendJSON(req, d, 400);
      }

      // instrumentation
      uint32_t nz = 0;
      uint32_t first[10];
      uint8_t firstN = 0;
      for (uint32_t i = 0; i < ledCount(); ++i) {
        uint8_t r = payload[i * 3 + 0], g = payload[i * 3 + 1], b = payload[i * 3 + 2];
        if ((r | g | b) != 0) {
          if (firstN < 10) first[firstN++] = i;
          ++nz;
        }
      }
      LOGI("frame stats: nonzero=%u (expect ~%u for a single column/row); first=[%s]",
           (unsigned)nz, (unsigned)fullH(),
           String([&]() {
             String s;
             for (uint8_t k = 0; k < firstN; k++) {
               if (k) s += ",";
               s += String(first[k]);
             }
             return s;
           }())
             .c_str());

      ensureStrip();
      strip->clear();
      for (uint32_t i = 0; i < ledCount(); ++i) {
        uint32_t si = lut[i];
        uint8_t r = payload[i * 3 + 0];
        uint8_t g = payload[i * 3 + 1];
        uint8_t b = payload[i * 3 + 2];
        if (si != 0xFFFFFFFF && si < strip->numPixels())
          strip->setPixelColor(si, strip->Color(r, g, b));
      }
      strip->show();

      DynamicJsonDocument ok(64);
      ok["ok"] = true;
      sendJSON(req, ok, 200);
    },
    NULL, onBody);

  // ---- Pixel fallback ----
  server.on("/api/pixel", HTTP_POST, [](AsyncWebServerRequest* req) {
    if (!req->hasParam("plain", true)) return sendError(req, 400, "no json");
    DynamicJsonDocument d(256);
    if (deserializeJson(d, req->getParam("plain", true)->value())) return sendError(req, 400, "bad json");
    uint16_t x = d["x"] | 0, y = d["y"] | 0;
    uint8_t r = d["r"] | 0, g = d["g"] | 0, b = d["b"] | 0;
    x = (uint16_t)clampu(x, 0, fullW() - 1);
    y = (uint16_t)clampu(y, 0, fullH() - 1);
    uint32_t si = lut[canvasIndex(x, y)];
    ensureStrip();
    if (si != 0xFFFFFFFF && si < strip->numPixels()) {
      strip->setPixelColor(si, strip->Color(r, g, b));
      strip->show();
      frameBody.clear();          // prevent residue causing "bad length"
      frameBody.shrink_to_fit();  // optional: free if we keep seeing reboots

      DynamicJsonDocument ok(64);
      ok["ok"] = true;
      sendJSON(req, ok, 200);
    } else sendError(req, 500, "lut hole");
  });

  // ---- Tests ----
  server.on("/api/test/row", HTTP_POST, [](AsyncWebServerRequest* req) {
    uint16_t y = req->getParam("y", true) ? req->getParam("y", true)->value().toInt() : 0;
    ensureStrip();
    strip->clear();
    y = (uint16_t)clampu(y, 0, fullH() - 1);
    for (uint16_t x = 0; x < fullW(); ++x) {
      uint32_t si = lut[canvasIndex(x, y)];
      if (si != 0xFFFFFFFF) strip->setPixelColor(si, strip->Color(0, 0, 255));
    }
    strip->show();
    DynamicJsonDocument ok(32);
    ok["ok"] = true;
    sendJSON(req, ok, 200);
  });
  server.on("/api/test/col", HTTP_POST, [](AsyncWebServerRequest* req) {
    uint16_t x = req->getParam("x", true) ? req->getParam("x", true)->value().toInt() : 0;
    ensureStrip();
    strip->clear();
    x = (uint16_t)clampu(x, 0, fullW() - 1);
    for (uint16_t y = 0; y < fullH(); ++y) {
      uint32_t si = lut[canvasIndex(x, y)];
      if (si != 0xFFFFFFFF) strip->setPixelColor(si, strip->Color(255, 0, 0));
    }
    strip->show();
    DynamicJsonDocument ok(32);
    ok["ok"] = true;
    sendJSON(req, ok, 200);
  });

  // **NEW**: Light a specific physical strip index
  server.on("/api/test/si", HTTP_POST, [](AsyncWebServerRequest* req) {
    uint32_t si = req->getParam("si", true) ? req->getParam("si", true)->value().toInt() : 0;
    ensureStrip();
    strip->clear();
    if (si < strip->numPixels()) strip->setPixelColor(si, strip->Color(255, 255, 255));
    strip->show();
    DynamicJsonDocument ok(32);
    ok["ok"] = true;
    sendJSON(req, ok, 200);
  });

  // **NEW**: Walk the physical strip 0..N-1
  server.on("/api/test/scan", HTTP_POST, [](AsyncWebServerRequest* req) {
    ensureStrip();
    for (uint32_t si = 0; si < strip->numPixels(); ++si) {
      strip->clear();
      strip->setPixelColor(si, strip->Color(255, 255, 255));
      strip->show();
      delay(30);
    }
    DynamicJsonDocument ok(32);
    ok["ok"] = true;
    sendJSON(req, ok, 200);
  });

  // ---- LittleFS basics ----
  server.on("/api/spiffs/list", HTTP_GET, [](AsyncWebServerRequest* req) {
    DynamicJsonDocument d(4096);
    JsonArray a = d.createNestedArray("files");
    File root = LittleFS.open("/");
    if (!root || !root.isDirectory()) return sendError(req, 500, "fs error");
    File f = root.openNextFile();
    while (f) {
      JsonObject o = a.createNestedObject();
      String n = f.name();
      if (n.startsWith("/")) n.remove(0, 1);
      o["name"] = n;
      o["size"] = f.size();
      f = root.openNextFile();
    }
    sendJSON(req, d, 200);
  });
  server.on("/api/spiffs/delete", HTTP_POST, [](AsyncWebServerRequest* req) {
    if (!req->hasParam("filename", true)) return sendError(req, 400, "filename missing");
    String n = req->getParam("filename", true)->value();
    if (!n.startsWith("/")) n = "/" + n;
    if (!LittleFS.exists(n)) return sendError(req, 404, "not found or remove failed");
    bool ok = LittleFS.remove(n);
    if (!ok) return sendError(req, 404, "not found or remove failed");
    DynamicJsonDocument d(64);
    d["ok"] = true;
    sendJSON(req, d, 200);
  });
  server.on(
    "/api/spiffs/upload", HTTP_POST,
    [](AsyncWebServerRequest* req) {
      DynamicJsonDocument d(64);
      d["ok"] = true;
      sendJSON(req, d, 200);
    },
    [](AsyncWebServerRequest* req, String filename, size_t index, uint8_t* data, size_t len, bool final) {
      if (index == 0) {
        if (filename.startsWith("/")) filename.remove(0, 1);
        req->_tempFile = LittleFS.open("/" + filename, "w");
        LOGI("upload start '%s'", filename.c_str());
      }
      if (req->_tempFile) req->_tempFile.write(data, len);
      if (final) {
        if (req->_tempFile) req->_tempFile.close();
        LOGI("upload done '%s' bytes=%u", filename.c_str(), (unsigned)(index + len));
      }
    });
  server.on("/api/spiffs/clear", HTTP_POST, [](AsyncWebServerRequest* req) {
    LittleFS.end();
    bool ok = LittleFS.begin(true);
    if (!ok) return sendError(req, 500, "format failed");
    DynamicJsonDocument d(64);
    d["ok"] = true;
    sendJSON(req, d, 200);
  });

  // ---- WiFi config ----
  server.on("/api/wifi/config", HTTP_GET, [](AsyncWebServerRequest* req) {
    DynamicJsonDocument d(256);
    d["sta_ssid"] = staSSID;
    d["sta_ok"] = (WiFi.status() == WL_CONNECTED);
    d["sta_ip"] = WiFi.localIP().toString();
    d["ap_ip"] = WiFi.softAPIP().toString();
    sendJSON(req, d, 200);
  });
  server.on("/api/wifi/config", HTTP_POST, [](AsyncWebServerRequest* req) {
    if (!req->hasParam("plain", true)) return sendError(req, 400, "no json");
    DynamicJsonDocument d(512);
    if (deserializeJson(d, req->getParam("plain", true)->value())) return sendError(req, 400, "bad json");
    staSSID = d["ssid"] | "";
    staPASS = d["pass"] | "";
    saveWiFiJson();
    trySTA();
    DynamicJsonDocument ok(64);
    ok["ok"] = true;
    sendJSON(req, ok, 200);
  });

  // ---- Static/default ----
  server.serveStatic("/", LittleFS, "/");
  server.on("/", HTTP_GET, [](AsyncWebServerRequest* req) {
    if (LittleFS.exists("/index.html")) req->send(LittleFS, "/index.html", "text/html");
    else req->send(200, "text/plain", "PanelPro is running.\nUpload a frontend to /index.html.");
  });

  // ---- CORS preflight fallback + 404 ----
  server.onNotFound([](AsyncWebServerRequest* req) {
    if (req->method() == HTTP_OPTIONS) {
      auto* res = req->beginResponse(204);
      res->addHeader("Access-Control-Allow-Origin", "*");
      res->addHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
      res->addHeader("Access-Control-Allow-Headers", "*");
      req->send(res);
      return;
    }
    req->send(404, "text/plain", "Not found");
  });

  // WS events (send ring buffer on connect)
  ws.onEvent([](AsyncWebSocket* server, AsyncWebSocketClient* client, AwsEventType type,
                void* arg, uint8_t* data, size_t len) {
    if (type == WS_EVT_CONNECT) {
      for (auto& l : logRing) client->text(l);
    }
  });
  server.addHandler(&ws);
  ws.enable(true);
}

// ---------- Setup / Loop ----------
void setup() {
  Serial.begin(115200);
  delay(150);
  Serial.println();
  LOGI("%s v%s boot", FW_NAME, FW_VERSION);

  pinMode(LED_BUILTIN, OUTPUT);
  digitalWrite(LED_BUILTIN, 1);

  if (!LittleFS.begin(true)) LOGE("LittleFS mount failed");
  else LOGI("LittleFS mounted: %u/%u", (unsigned)LittleFS.usedBytes(), (unsigned)LittleFS.totalBytes());

  (void)loadPanelJson();
  ensureStrip();
  rebuildLUT();

  startAP();
  loadWiFiJson();
  trySTA();

  if (MDNS.begin("panelpro")) LOGI("mDNS started: http://panelpro.local");
  else LOGW("mDNS failed");

  setupRoutes();
  server.onRequestBody(onBody);
  server.begin();
  LOGI("HTTP server started");

  digitalWrite(LED_BUILTIN, 0);
}

void loop() {
  // nothing; Async handles everything
}
