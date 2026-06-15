import io
import os
import random
import time
import string
import json
import base64
from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for
from flask_socketio import SocketIO, emit
import paho.mqtt.client as mqtt

import barcode  # type: ignore
from barcode.writer import ImageWriter  # type: ignore

app = Flask(__name__)
app.config['SECRET_KEY'] = 'pos_secret_key_9981_secure_node'
# Session tidak permanen — menutup browser = logout otomatis
app.config['SESSION_PERMANENT'] = False
# Cookie session hanya berlaku 8 jam
from datetime import timedelta
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=8)

socketio = SocketIO(app, cors_allowed_origins="*", async_mode=None)

# ==========================================================
# KONFIGURASI HIVEMQ MQTT & FILE HISTORY
# ==========================================================
MQTT_BROKER       = "69a2de66684b4f3f8d1012aae920827d.s1.eu.hivemq.cloud"
MQTT_PORT         = 8883        # SSL/TLS
MQTT_USER         = "generic_test_webend_0"
MQTT_PASS         = "genericWeb0"
# Topic sesuai format alat: telemetry/vg/<device_id>/<kategori>
MQTT_TOPIC_PREFIX = "telemetry/vg"
MQTT_TOPIC_ALL    = "telemetry/vg/#"         # Wildcard — tangkap semua device & kategori
MQTT_TOPIC_TAMPER = "telemetry/vg/+/tamper"  # Event benturan/tamper
MQTT_TOPIC_ROUTINE= "telemetry/vg/+/routine" # Data GPS rutin
MQTT_TOPIC_SYSTEM = "telemetry/vg/+/system"  # Data sistem/baterai
MQTT_TOPIC_IMAGE  = "telemetry/vg/+/image"   # Gambar dari kamera
HISTORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'history.json')

# ─── State global ───
active_manifest = {
    "cust_name": "Belum Terdaftar",
    "cust_phone": "-",
    "address": "-",
    "item_type": "-",
    "item_value": "Rp 0",
    "courier": "Belum Terpilih",
    "device_id": "NODE-HV-PENDING",
    "otp_token": None,
    "access_token": None,
    "started_at": "-"
}

current_telemetry = {
    "device_id": "NODE-HV-PENDING",
    "status": "OFFLINE (MENUNGGU ALAT)",
    "speed": "0",
    "lat": -6.885874,
    "lng": 107.538179,
    "last_seen": 0
}

safety_status = {
    "benturan": False,
    "waktu_kejadian": "-",
    "pemaksaan_buka": False
}

# ─── Evidence ───
EVIDENCE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'evidence')
os.makedirs(EVIDENCE_DIR, exist_ok=True)
captured_evidence  = []
last_evidence_meta = {"trigger": "-", "time": "-", "count": 0}


# ==========================================================
# HELPERS
# ==========================================================
def _load_history():
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

def _save_history(data):
    try:
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"❌ Gagal menulis history.json: {e}")

def _generate_barcode_base64(text):
    try:
        from io import BytesIO
        rv = BytesIO()
        CODE128 = barcode.get_by_name('code128', writer=ImageWriter())
        CODE128.write(text, options={"write_text": True, "font_size": 10, "text_distance": 4, "module_height": 15.0}).save(rv)
        return "data:image/png;base64," + base64.b64encode(rv.getvalue()).decode('utf-8')
    except Exception as e:
        print(f"❌ Gagal generate barcode: {e}")
        return ""

def _save_image_bytes(raw):
    if not raw:
        return None
    fname = f"evd_{int(time.time()*1000)}_{random.randint(100,999)}.jpg"
    with open(os.path.join(EVIDENCE_DIR, fname), 'wb') as out:
        out.write(raw)
    return url_for('static', filename=f'evidence/{fname}')

def _save_b64_image(b64):
    if not b64:
        return None
    if isinstance(b64, str) and b64.strip().startswith('data:') and ',' in b64:
        b64 = b64.split(',', 1)[1]
    try:
        raw = base64.b64decode(b64)
    except Exception:
        return None
    return _save_image_bytes(raw)

def _clear_evidence_files():
    try:
        for fn in os.listdir(EVIDENCE_DIR):
            if fn.startswith('evd_'):
                os.remove(os.path.join(EVIDENCE_DIR, fn))
    except Exception:
        pass


# ==========================================================
# BACKGROUND HEARTBEAT
# ==========================================================
def check_device_heartbeat():
    global current_telemetry
    while True:
        socketio.sleep(2)
        if current_telemetry["last_seen"] > 0:
            if time.time() - current_telemetry["last_seen"] > 30:
                if "ONLINE" in current_telemetry["status"]:
                    print("⚠️ Hardware terputus. Status → OFFLINE.")
                    current_telemetry["status"] = "❌ OFFLINE (ALAT MATI)"
                    current_telemetry["speed"] = "0"
                    socketio.emit('ui_refresh', {**current_telemetry, **safety_status})


# ==========================================================
# MQTT CLIENT
# ==========================================================
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"📡 Terhubung ke HiveMQ — kode: {rc}")
        # Subscribe wildcard — tangkap semua topic dari semua device
        client.subscribe(MQTT_TOPIC_ALL)
        print(f"📡 Subscribe ke: {MQTT_TOPIC_ALL}")
        print(f"📡 Mendengarkan: tamper | routine | system | image")
    else:
        print(f"⚠️ Gagal connect HiveMQ — kode: {rc}")

def on_message(client, userdata, msg):
    global current_telemetry, safety_status, captured_evidence, last_evidence_meta
    try:
        # ── Deteksi jenis topic dari alat ──
        topic_parts = msg.topic.split('/')
        # Format: telemetry/vg/<device_id>/<kategori>
        kategori = topic_parts[-1] if len(topic_parts) >= 4 else "unknown"

        if kategori == "image":
            _handle_mqtt_image(msg.payload)
            return

        data = json.loads(msg.payload.decode('utf-8'))
        print(f"📥 Telemetri MQTT [{msg.topic}]: event={data.get('event_type','?')} seq={data.get('seq_id','?')}")

        # ── Parse GPS dari format alat IoT ──
        gps   = data.get("gps", {})
        imu   = data.get("imu", {})
        env   = data.get("environment", {})
        cam   = data.get("camera", {})

        # GPS — bisa "N/A" kalau fix belum dapat
        raw_lat = gps.get("lat", "N/A")
        raw_lng = gps.get("lng", "N/A")
        raw_spd = gps.get("speed_kmh", "N/A")
        gps_fix = gps.get("fix_valid", False)

        try:    lat_val = float(raw_lat)
        except: lat_val = current_telemetry.get("lat", -6.885874)

        try:    lng_val = float(raw_lng)
        except: lng_val = current_telemetry.get("lng", 107.538179)

        try:    spd_val = str(int(float(raw_spd)))
        except: spd_val = "0"

        # Status berdasarkan GPS fix
        if gps_fix:
            status_str = "🟢 ONLINE (GPS LOCK)"
        else:
            status_str = "🟡 ONLINE (GPS MENCARI SINYAL)"

        # Update device_id dari alat langsung
        real_device_id = data.get("device_id", active_manifest["device_id"])

        current_telemetry.update({
            "device_id": real_device_id,
            "status":    status_str,
            "speed":     spd_val,
            "lat":       lat_val,
            "lng":       lng_val,
            "last_seen": time.time(),
            # Data tambahan dari IMU & environment
            "battery":   env.get("battery_pct", "-"),
            "temp_c":    imu.get("temp_c", "-"),
            "shock":     imu.get("shock_str", "NONE"),
            "tilt_roll": round(imu.get("tilt", {}).get("roll_deg", 0), 1),
            "tilt_pitch":round(imu.get("tilt", {}).get("pitch_deg", 0), 1),
            "sats":      gps.get("satellites", "N/A"),
        })

        # ── Event type → safety status ──
        event_type = data.get("event_type", "")
        shock_str  = imu.get("shock_str", "NONE")

        # Semua event type yang dikenal
        TAMPER_EVENTS = ("TAMPER_WITH_IMAGE", "TAMPER", "SHOCK_DETECTED",
                         "TAMPER_DEVICE_REMOVED", "TAMPER_DETECTED")
        OPEN_EVENTS   = ("TAMPER_WITH_IMAGE", "TAMPER", "OPEN_DETECTED",
                         "TAMPER_DEVICE_REMOVED")

        if event_type in TAMPER_EVENTS or shock_str not in ("NONE", "-", ""):
            safety_status["benturan"] = True
            if safety_status["waktu_kejadian"] == "-":
                safety_status["waktu_kejadian"] = time.strftime("%H:%M:%S WIB")

        if event_type in OPEN_EVENTS:
            safety_status["pemaksaan_buka"] = True

        # Selalu emit ke semua client — update dashboard
        payload_emit = {**current_telemetry, **safety_status}
        socketio.emit('ui_refresh', payload_emit)
        print(f"📡 Emit ui_refresh → lat={lat_val} lng={lng_val} status={status_str}")

        # ── Gambar inline di payload (jpeg_b64) ──
        jpeg_b64 = cam.get("jpeg_b64", "")
        if jpeg_b64 and len(jpeg_b64) > 100:
            print(f"📸 Gambar inline ditemukan ({len(jpeg_b64)} chars b64)")
            saved = _save_b64_image(jpeg_b64)
            if saved:
                _append_evidence(saved, event_type or "MQTT_INLINE", data.get("device_id", "-"))

        # ── Jika delivery:"separate_topic" → tunggu topic image ──
        elif cam.get("delivery") == "separate_topic":
            print(f"📡 Gambar akan datang via topic terpisah ({cam.get('frame_count',0)} frame)")

    except Exception as e:
        print(f"❌ Gagal proses MQTT: {e}")


def _handle_mqtt_image(payload):
    """Handle gambar yang datang via topic pos/secure/telemetri/image."""
    global captured_evidence, last_evidence_meta
    try:
        # Coba parse sebagai JSON dulu (mungkin ada wrapper)
        try:
            obj      = json.loads(payload.decode('utf-8'))
            b64_data = obj.get("jpeg_b64") or obj.get("image") or obj.get("data", "")
            trigger  = obj.get("event_type", "MQTT_IMAGE")
            dev_id   = obj.get("device_id", "-")
        except Exception:
            # Payload langsung base64 string
            b64_data = payload.decode('utf-8')
            trigger  = "MQTT_IMAGE"
            dev_id   = "-"

        if b64_data and len(b64_data) > 100:
            saved = _save_b64_image(b64_data)
            if saved:
                _append_evidence(saved, trigger, dev_id)
                print(f"✅ Gambar dari topic image disimpan: {saved}")
        else:
            # Payload mungkin raw JPEG bytes
            saved = _save_image_bytes(payload)
            if saved:
                _append_evidence(saved, "MQTT_IMAGE_RAW", "-")
                print(f"✅ Gambar raw bytes disimpan: {saved}")
    except Exception as e:
        print(f"❌ Gagal proses gambar MQTT: {e}")


def _append_evidence(url, trigger, dev_id):
    """Tambahkan foto ke captured_evidence dan emit ke semua client."""
    global captured_evidence, last_evidence_meta
    captured_evidence = (captured_evidence + [url])[-5:]
    last_evidence_meta = {
        "trigger": trigger,
        "time":    time.strftime("%H:%M:%S WIB"),
        "count":   len(captured_evidence)
    }
    socketio.emit('evidence_update', {
        "photos":  captured_evidence,
        "trigger": trigger,
        "time":    last_evidence_meta["time"],
        "count":   len(captured_evidence)
    })
    print(f"📸 Evidence update: {len(captured_evidence)} foto | trigger={trigger}")

def start_mqtt_background():
    import ssl
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
        print(f"📡 Konek ke {MQTT_BROKER}:{MQTT_PORT} (SSL/TLS + Auth)")
    except Exception as e:
        print(f"⚠️ Gagal konek HiveMQ: {e}")


# ==========================================================
# ROUTES — HALAMAN ADMIN
# ==========================================================
@app.route('/')
def login_page():
    """
    Halaman login khusus admin.
    Selalu tampilkan login screen — admin harus input password.
    Session admin dibersihkan agar tidak auto-login dari cookie lama.
    """
    # Paksa logout admin setiap kali halaman utama dibuka
    # Ini memastikan admin selalu harus login ulang
    session.pop('admin_logged', None)
    return render_template(
        'index.html',
        mode='login',
        active_manifest_admin=active_manifest,
        current_telemetry=current_telemetry,
        is_admin_logged=False  # Selalu False — login screen selalu muncul
    )


# ==========================================================
# ROUTES — PORTAL PELACAKAN USER (TERPISAH)
# ==========================================================
@app.route('/user')
def user_portal():
    """
    /user sekarang tidak dipakai langsung.
    User harus akses via link QR: /track/<token>/verify
    Tampilkan halaman info jika ada yang akses langsung.
    """
    return render_template('user.html', token=None, token_valid=None)


@app.route('/track/<token>/verify')
def user_otp_page(token):
    """
    Halaman input OTP khusus per token unik.
    Hanya muncul kalau token valid (ada di active_manifest).
    """
    # Kalau token tidak dikenal, tampilkan halaman error
    if active_manifest.get('access_token') != token:
        return render_template('user.html', token=token, token_valid=False)
    return render_template('user.html', token=token, token_valid=True)


@app.route('/track/<token>')
def track_page(token):
    """
    Dashboard tracking real-time untuk user.
    Token di URL harus cocok dengan access_token aktif.
    OTP juga harus sudah diverifikasi via session.
    """
    # Validasi 1: token di URL harus cocok dengan access_token yang digenerate
    if active_manifest.get('access_token') != token:
        return redirect(url_for('user_otp_page', token=token))

    # Validasi 2: session OTP harus sudah terverifikasi
    if (
        'user_otp' not in session
        or session['user_otp'] != active_manifest['otp_token']
        or active_manifest['otp_token'] is None
    ):
        session.pop('user_otp', None)
        return redirect(url_for('user_otp_page', token=token))

    return render_template(
        'tracking.html',
        package_name=active_manifest['item_type'],
        cust_name=active_manifest['cust_name'],
        cust_phone=active_manifest['cust_phone'],
        address=active_manifest['address'],
        item_type=active_manifest['item_type'],
        item_value=active_manifest['item_value'],
        courier=active_manifest['courier'],
        current_telemetry=current_telemetry,
        safety=safety_status,
        photos=captured_evidence,
        evidence_meta=last_evidence_meta
    )


# ==========================================================
# API — AUTENTIKASI
# ==========================================================
@app.route('/api/admin-login', methods=['POST'])
def admin_login_api():
    data = request.get_json()
    if data.get('code', '') == "ADMIN789":
        session['admin_logged'] = True
        return jsonify({
            "status": "success",
            "manifest": active_manifest,
            "telemetry": current_telemetry,
            "safety": safety_status
        })
    return jsonify({"status": "error", "message": "Kode admin salah"}), 401

@app.route('/api/admin-logout', methods=['POST'])
def admin_logout_api():
    session.pop('admin_logged', None)
    return jsonify({"status": "success"})

@app.route('/api/verify-otp', methods=['POST'])
def verify_otp():
    """
    Validasi OTP dari halaman /track/<token>/verify.
    Token dikirim bersama OTP untuk verifikasi ganda.
    """
    data = request.get_json()
    input_otp   = data.get('otp', '').strip().upper()
    input_token = data.get('token', '').strip()

    # Validasi token URL cocok dengan yang aktif
    if active_manifest.get('access_token') != input_token:
        return jsonify({"status": "error", "message": "Link tidak valid"}), 401

    # Validasi OTP
    if active_manifest['otp_token'] and input_otp == active_manifest['otp_token']:
        session['user_otp'] = input_otp
        tracking_url = url_for('track_page', token=input_token, _external=False)
        return jsonify({"status": "success", "url": tracking_url})

    return jsonify({"status": "error", "message": "OTP salah atau kedaluwarsa"}), 401

@app.route('/api/user-logout', methods=['POST'])
def user_logout():
    session.pop('user_otp', None)
    return jsonify({"status": "success"})


# ==========================================================
# API — TELEMETRI (HTTP BACKUP)
# ==========================================================
@app.route('/api/telemetri', methods=['POST'])
def receive_telemetry():
    global current_telemetry, safety_status
    data = request.get_json()
    if not data:
        return jsonify({"status": "error"}), 400

    current_telemetry.update({
        "device_id": active_manifest["device_id"],
        "status": "🟢 ONLINE (GPS LOCK)",
        "speed": str(data.get("speed", "0")),
        "lat": float(data.get("lat", -6.885874)),
        "lng": float(data.get("lng", 107.538179)),
        "last_seen": time.time()
    })
    safety_status["benturan"] = data.get("benturan", False)
    safety_status["pemaksaan_buka"] = data.get("pemaksaan_buka", False)
    if safety_status["benturan"] and safety_status["waktu_kejadian"] == "-":
        safety_status["waktu_kejadian"] = time.strftime("%H:%M:%S WIB")

    socketio.emit('ui_refresh', {**current_telemetry, **safety_status})
    return jsonify({"status": "success"})


# ==========================================================
# API — UPLOAD FOTO BUKTI
# ==========================================================
@app.route('/api/upload-evidence', methods=['POST'])
def upload_evidence():
    global captured_evidence, last_evidence_meta
    new_saved = []
    trigger = None

    if request.content_type and 'multipart/form-data' in request.content_type:
        trigger = request.form.get('trigger')
        if str(request.form.get('new_event', '')).lower() in ('1', 'true'):
            _clear_evidence_files(); captured_evidence = []
        for f in request.files.getlist('photos'):
            saved = _save_image_bytes(f.read())
            if saved: new_saved.append(saved)
    else:
        data = request.get_json(silent=True) or {}
        trigger = data.get('trigger')
        if data.get('new_event'):
            _clear_evidence_files(); captured_evidence = []
        if isinstance(data.get('images'), list):
            for b64 in data['images']:
                saved = _save_b64_image(b64)
                if saved: new_saved.append(saved)
        elif data.get('image'):
            saved = _save_b64_image(data['image'])
            if saved: new_saved.append(saved)

    if not new_saved:
        return jsonify({"status": "error", "message": "Tidak ada foto valid"}), 400

    captured_evidence = (captured_evidence + new_saved)[-5:]
    last_evidence_meta = {
        "trigger": trigger or "Sensor Terpicu",
        "time": time.strftime("%H:%M:%S WIB"),
        "count": len(captured_evidence)
    }
    socketio.emit('evidence_update', {"photos": captured_evidence, **last_evidence_meta})
    return jsonify({"status": "success", "count": len(captured_evidence), "photos": captured_evidence})


# ==========================================================
# API — GENERATE TOKEN & QR
# ==========================================================
@app.route('/api/generate-token', methods=['POST'])
def generate_token():
    global active_manifest, current_telemetry, captured_evidence, last_evidence_meta
    data = request.get_json()

    generated_id     = f"NODE-HV-{random.randint(1000,9999)}X"
    generated_otp    = ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))
    # Token URL random 12 karakter — unik per sesi pengiriman
    generated_token  = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))

    active_manifest = {
        "cust_name":     data.get('cust_name', 'No Name'),
        "cust_phone":    data.get('cust_phone', '-'),
        "address":       data.get('address', '-'),
        "item_type":     data.get('item_type', '-'),
        "item_value":    data.get('item_value', 'Rp 0'),
        "courier":       data.get('courier', 'Regular'),
        "device_id":     generated_id,
        "otp_token":     generated_otp,
        "access_token":  generated_token,
        "started_at":    time.strftime("%d/%m/%Y %H:%M WIB")
    }

    _clear_evidence_files()
    captured_evidence  = []
    last_evidence_meta = {"trigger": "-", "time": "-", "count": 0}
    socketio.emit('evidence_update', {"photos": [], "trigger": "-", "time": "-", "count": 0})

    current_telemetry.update({
        "device_id": generated_id,
        "status":    "❌ OFFLINE (ALAT MATI)",
        "speed":     "0",
        "last_seen": 0
    })

    # QR mengarah ke halaman OTP yang unik per sesi: /track/<token>/verify
    user_url = url_for('user_otp_page', token=generated_token, _external=True)

    print(f"✅ Token: OTP={generated_otp} | Device={generated_id} | AccessToken={generated_token} | URL={user_url}")

    return jsonify({
        "status":       "success",
        "url":          user_url,
        "user_url":     user_url,
        "otp":          generated_otp,
        "device_id":    generated_id,
        "access_token": generated_token
    })


# ==========================================================
# API — SELESAIKAN / RESET PELACAKAN
# ==========================================================
@app.route('/api/selesaikan-pelacakan', methods=['POST'])
def reset_tracking_node():
    global active_manifest, current_telemetry, safety_status, captured_evidence, last_evidence_meta

    if active_manifest.get("otp_token") is not None:
        history_list = _load_history()

        # Salin file foto ke folder history agar tetap tersedia setelah reset
        import shutil
        HISTORY_PHOTO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'history_photos')
        os.makedirs(HISTORY_PHOTO_DIR, exist_ok=True)
        saved_photo_urls = []
        for photo_url in captured_evidence:
            try:
                # photo_url = /static/evidence/evd_xxx.jpg
                fname = os.path.basename(photo_url.split("?")[0])
                src   = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'evidence', fname)
                dst   = os.path.join(HISTORY_PHOTO_DIR, f"{active_manifest['device_id']}_{fname}")
                if os.path.exists(src):
                    shutil.copy2(src, dst)
                    saved_photo_urls.append(url_for('static', filename=f'history_photos/{active_manifest["device_id"]}_{fname}'))
            except Exception as e:
                print(f"Gagal salin foto: {e}")

        history_list.append({
            "device_id":      active_manifest["device_id"],
            "otp_token":      active_manifest["otp_token"],
            "cust_name":      active_manifest["cust_name"],
            "cust_phone":     active_manifest["cust_phone"],
            "address":        active_manifest["address"],
            "item_type":      active_manifest["item_type"],
            "item_value":     active_manifest["item_value"],
            "courier":        active_manifest["courier"],
            "started_at":     active_manifest.get("started_at", "-"),
            "ended_at":       time.strftime("%d/%m/%Y %H:%M WIB"),
            "last_location":  [current_telemetry["lat"], current_telemetry["lng"]],
            "last_speed":     current_telemetry.get("speed", "0"),
            "safety_summary": {
                "benturan":       safety_status["benturan"],
                "waktu_benturan": safety_status["waktu_kejadian"],
                "pemaksaan_buka": safety_status["pemaksaan_buka"]
            },
            "total_photos":   len(captured_evidence),
            "photo_urls":     saved_photo_urls,  # URL foto yang disimpan permanen
            "evidence_trigger": last_evidence_meta.get("trigger", "-"),
            "evidence_time":    last_evidence_meta.get("time", "-"),
        })
        _save_history(history_list)

    # Simpan device_id SEBELUM manifest di-reset, untuk dikirim ke user
    finished_device_id = active_manifest["device_id"]
    finished_ended_at  = time.strftime("%d/%m/%Y %H:%M WIB")

    session.pop('user_otp', None)
    _clear_evidence_files()
    captured_evidence  = []
    last_evidence_meta = {"trigger": "-", "time": "-", "count": 0}

    active_manifest = {
        "cust_name": "Belum Terdaftar", "cust_phone": "-", "address": "-",
        "item_type": "-", "item_value": "Rp 0", "courier": "Belum Terpilih",
        "device_id": "NODE-HV-PENDING", "otp_token": None, "access_token": None, "started_at": "-"
    }
    current_telemetry = {
        "device_id": "NODE-HV-PENDING", "status": "OFFLINE (MENUNGGU ALAT)",
        "speed": "0", "lat": -6.885874, "lng": 107.538179, "last_seen": 0
    }
    safety_status = {"benturan": False, "waktu_kejadian": "-", "pemaksaan_buka": False}

    socketio.emit('ui_refresh',     {**current_telemetry, **safety_status})
    socketio.emit('evidence_update', {"photos": [], "trigger": "-", "time": "-", "count": 0})
    # Kirim device_id ke frontend agar user bisa unduh invoice dari arsip history
    socketio.emit('tracking_ended', {
        "message": "Pelacakan telah diakhiri oleh petugas.",
        "ended_at": finished_ended_at,
        "device_id": finished_device_id
    })
    return jsonify({"status": "success", "message": "Pelacakan dihentikan & diarsipkan ke riwayat."})


# ==========================================================
# API — RIWAYAT
# ==========================================================
@app.route('/api/history', methods=['GET'])
def get_history_api():
    if not session.get('admin_logged'):
        return jsonify({"status": "error", "message": "Akses ditolak"}), 401
    return jsonify(_load_history())

@app.route('/api/history/<device_id>', methods=['DELETE'])
def delete_history_item(device_id):
    if not session.get('admin_logged'):
        return jsonify({"status": "error", "message": "Akses ditolak"}), 401
    history_list = _load_history()
    _save_history([i for i in history_list if i["device_id"] != device_id])
    return jsonify({"status": "success", "message": f"Log {device_id} dihapus."})


# ==========================================================
# ENDPOINT — LAPORAN PENGIRIMAN PDF (ReportLab, Windows-compatible)
# ==========================================================
@app.route('/download-invoice')
@app.route('/download-invoice/<device_id>')
def download_invoice(device_id=None):
    """Laporan Pengiriman resmi bergambar logo Pos Indonesia + foto bukti kondisi paket."""
    from reportlab.lib.pagesizes import A4  # type: ignore
    from reportlab.lib import colors  # type: ignore
    from reportlab.lib.units import mm  # type: ignore
    from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,  # type: ignore
                                    Paragraph, Spacer, HRFlowable, Image as RLImage)
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle  # type: ignore
    from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT, TA_JUSTIFY  # type: ignore
    import os as _os, tempfile

    # ── Ambil data ──
    target_data = None
    if device_id:
        for item in _load_history():
            if item.get("device_id") == device_id:
                target_data = item; break
        if not target_data:
            return jsonify({"status": "error", "message": "Data tidak ditemukan"}), 404
    else:
        if not active_manifest or active_manifest.get("otp_token") is None:
            return jsonify({"status": "error", "message": "Tidak ada sesi aktif"}), 400
        target_data = active_manifest

    dev_id   = target_data.get("device_id", "UNKNOWN")
    inv_no   = f"POS/LPG/{time.strftime('%Y')}/{dev_id[-6:]}"
    tanggal  = time.strftime("%d %B %Y")

    # Foto bukti — dari history atau dari sesi aktif
    photo_urls = target_data.get("photo_urls", [])
    if not photo_urls and not device_id:
        photo_urls = captured_evidence  # sesi aktif

    # Konversi URL ke path fisik di disk
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    def url_to_path(url):
        # /static/evidence/xxx.jpg  →  BASE_DIR/static/evidence/xxx.jpg
        rel = url.split("/static/", 1)[-1] if "/static/" in url else url
        return _os.path.join(BASE_DIR, "static", rel)

    photo_paths = [url_to_path(u) for u in photo_urls
                   if _os.path.exists(url_to_path(u))]

    # ── Palet ──
    ORANGE     = colors.HexColor("#FF6B00")
    ORANGE_LT  = colors.HexColor("#FF9A4D")
    ORANGE_PAL = colors.HexColor("#FFF3E8")
    ORANGE_XP  = colors.HexColor("#FFFAF5")
    DARK       = colors.HexColor("#1A0E07")
    SLATE      = colors.HexColor("#4A3728")
    MUTED      = colors.HexColor("#8B6E5A")
    BORDER     = colors.HexColor("#E8D5C4")
    GREEN      = colors.HexColor("#16A34A")
    RED        = colors.HexColor("#DC2626")
    LIGHT_BG   = colors.HexColor("#F9F4EF")
    WHITE      = colors.white

    pdf_stream = io.BytesIO()
    W_PAGE, H_PAGE = A4
    MARGIN = 18 * mm
    W = W_PAGE - 2 * MARGIN

    doc = SimpleDocTemplate(
        pdf_stream, pagesize=A4,
        rightMargin=MARGIN, leftMargin=MARGIN,
        topMargin=15*mm, bottomMargin=18*mm,
        title=f"Laporan Data Pengiriman Pos Indonesia — {dev_id}"
    )

    styles = getSampleStyleSheet()

    def P(text, size=9, bold=False, color=DARK, align=TA_LEFT,
          leading=None, italic=False, space_after=0):
        fn = ("Helvetica-BoldOblique" if bold and italic else
              "Helvetica-Bold" if bold else
              "Helvetica-Oblique" if italic else "Helvetica")
        return Paragraph(str(text), ParagraphStyle(
            "x", parent=styles["Normal"],
            fontSize=size, fontName=fn, textColor=color,
            alignment=align, leading=leading or max(size * 1.45, 10),
            spaceAfter=space_after, spaceBefore=0
        ))

    story = []

    # ══════════════════════════════════════════════════════════
    # HEADER — Logo Pos kiri, judul tengah, info dokumen kanan
    # ══════════════════════════════════════════════════════════
    logo_path = _os.path.join(BASE_DIR, "static", "logo_pos.png")
    logo_cell = RLImage(logo_path, width=28*mm, height=28*mm) if _os.path.exists(logo_path) else P("POS", 16, True, ORANGE, TA_CENTER)

    header_data = [[
        logo_cell,
        [
            P("LAPORAN DATA PENGIRIMAN", 16, True, DARK),
            P("PT Pos Indonesia (Persero) — Bukti &amp; Rekaman Kargo Berharga", 8, False, ORANGE, italic=True),
            Spacer(1, 2*mm),
            P("Sentral Pengolahan Pos (SPP) Bandung 40000, Jawa Barat", 7.5, False, MUTED),
        ],
        [
            P(f"No Dokumen:", 7, False, MUTED, TA_RIGHT),
            P(f"<b>{inv_no}</b>", 9, True, DARK, TA_RIGHT),
            Spacer(1, 1*mm),
            P(f"Tanggal Cetak:", 7, False, MUTED, TA_RIGHT),
            P(f"<b>{tanggal}</b>", 9, True, DARK, TA_RIGHT),
            Spacer(1, 1*mm),
            P(f"ID Node:", 7, False, MUTED, TA_RIGHT),
            P(f"<b>{dev_id}</b>", 8, True, ORANGE, TA_RIGHT),
        ],
    ]]
    header_tbl = Table(header_data, colWidths=[30*mm, W - 30*mm - 50*mm, 50*mm])
    header_tbl.setStyle(TableStyle([
        ("VALIGN",       (0,0),(-1,-1), "MIDDLE"),
        ("LEFTPADDING",  (1,0),(1,0),   8),
        ("RIGHTPADDING", (2,0),(2,0),   0),
        ("BOTTOMPADDING",(0,0),(-1,-1), 6),
        ("LINEBELOW",    (0,0),(-1,-1), 3, ORANGE),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 4*mm))

    # Strip orange
    story.append(Table([[""]], colWidths=[W],
        style=TableStyle([("BACKGROUND",(0,0),(0,0), ORANGE_LT),
                          ("ROWHEIGHT",(0,0),(0,0), 2)])))
    story.append(Spacer(1, 5*mm))

    # ══════════════════════════════════════════════════════════
    # SEKSI 1 — IDENTITAS PENGIRIMAN
    # ══════════════════════════════════════════════════════════
    story.append(P("■  IDENTITAS PENGIRIMAN", 10, True, ORANGE, space_after=3))
    story.append(Spacer(1, 2*mm))

    col_w2 = (W - 5*mm) / 2
    id_left = [
        ["Nama Penerima",   target_data.get("cust_name", "-")],
        ["No. Telepon",     target_data.get("cust_phone", "-")],
        ["Alamat Tujuan",   target_data.get("address", "-")],
    ]
    id_right = [
        ["Jenis Muatan",    target_data.get("item_type", "-")],
        ["Nilai Barang",    target_data.get("item_value", "-")],
        ["Layanan Kurir",   target_data.get("courier", "-")],
    ]

    def make_kv_table(rows, bg=WHITE):
        data = [[P(k, 8, False, MUTED), P(f"<b>{v}</b>", 8, True, DARK)] for k,v in rows]
        t = Table(data, colWidths=[col_w2 * 0.42, col_w2 * 0.58])
        t.setStyle(TableStyle([
            ("ROWBACKGROUNDS",(0,0),(-1,-1),[bg, ORANGE_XP]),
            ("TOPPADDING",   (0,0),(-1,-1), 5),
            ("BOTTOMPADDING",(0,0),(-1,-1), 5),
            ("LEFTPADDING",  (0,0),(-1,-1), 6),
            ("LINEBELOW",    (0,0),(-1,-2), 0.5, BORDER),
            ("BOX",          (0,0),(-1,-1), 1,   BORDER),
        ]))
        return t

    id_tbl = Table([[make_kv_table(id_left), make_kv_table(id_right)]], colWidths=[col_w2, col_w2])
    id_tbl.setStyle(TableStyle([
        ("VALIGN",       (0,0),(-1,-1), "TOP"),
        ("LEFTPADDING",  (1,0),(1,0),   5),
    ]))
    story.append(id_tbl)
    story.append(Spacer(1, 5*mm))

    # ══════════════════════════════════════════════════════════
    # SEKSI 2 — DATA TELEMETRI & PERJALANAN
    # ══════════════════════════════════════════════════════════
    story.append(P("■  DATA TELEMETRI PERJALANAN", 10, True, ORANGE, space_after=3))
    story.append(Spacer(1, 2*mm))

    lat  = target_data.get("last_location", ["-", "-"])[0]
    lng  = target_data.get("last_location", ["-", "-"])[1]
    speed = target_data.get("last_speed", "0")

    try: lat_str = f"{float(lat):.6f}°"
    except: lat_str = str(lat)
    try: lng_str = f"{float(lng):.6f}°"
    except: lng_str = str(lng)

    tele_rows = [
        ["Waktu Registrasi",     target_data.get("started_at", "-")],
        ["Waktu Selesai",        target_data.get("ended_at", "-")],
        ["Koordinat Terakhir",   f"{lat_str}, {lng_str}"],
        ["Kecepatan Terakhir",   f"{speed} km/jam"],
        ["Kode OTP Akses",       target_data.get("otp_token", "-")],
    ]
    tele_data = [[P(k, 8, False, MUTED), P(f"<b>{v}</b>", 8, True, DARK)] for k,v in tele_rows]
    tele_tbl = Table(tele_data, colWidths=[W*0.35, W*0.65])
    tele_tbl.setStyle(TableStyle([
        ("ROWBACKGROUNDS",(0,0),(-1,-1),[WHITE, ORANGE_XP]),
        ("TOPPADDING",   (0,0),(-1,-1), 5),
        ("BOTTOMPADDING",(0,0),(-1,-1), 5),
        ("LEFTPADDING",  (0,0),(-1,-1), 8),
        ("LINEBELOW",    (0,0),(-1,-2), 0.5, BORDER),
        ("BOX",          (0,0),(-1,-1), 1, BORDER),
        ("LINEAFTER",    (0,0),(0,-1),  2, ORANGE),
        ("BACKGROUND",   (0,0),(0,-1),  ORANGE_PAL),
    ]))
    story.append(tele_tbl)
    story.append(Spacer(1, 5*mm))

    # ══════════════════════════════════════════════════════════
    # SEKSI 3 — STATUS KEAMANAN ENCLOSURE
    # ══════════════════════════════════════════════════════════
    story.append(P("■  STATUS KEAMANAN ENCLOSURE", 10, True, ORANGE, space_after=3))
    story.append(Spacer(1, 2*mm))

    safety = target_data.get("safety_summary", {})
    ada_benturan = safety.get("benturan", False)
    ada_buka     = safety.get("pemaksaan_buka", False)
    waktu_bnt    = safety.get("waktu_benturan", "-")

    # Status badge benturan
    bnt_color = RED if ada_benturan else GREEN
    bnt_text  = f"⚠  TERDETEKSI — Waktu: {waktu_bnt}" if ada_benturan else "✔  TIDAK ADA BENTURAN — Paket aman"
    buka_color = RED if ada_buka else GREEN
    buka_text  = "⚠  TERDETEKSI PEMBUKAAN PAKSA" if ada_buka else "✔  SEGEL TERKUNCI — Tidak ada pembukaan paksa"

    sec_rows = [
        [P("Sensor Benturan",     8, True, MUTED),  P(bnt_text,  8, True, bnt_color)],
        [P("Segel Enclosure",     8, True, MUTED),  P(buka_text, 8, True, buka_color)],
        [P("Total Foto Bukti",    8, True, MUTED),  P(f"<b>{target_data.get('total_photos', 0)} foto kondisi paket terdokumentasi</b>", 8, True, DARK)],
    ]
    if ada_benturan or ada_buka:
        sec_rows.append([P("Pemicu Kamera",  8, True, MUTED),
                         P(f"<b>{target_data.get('evidence_trigger', '-')} — {target_data.get('evidence_time', '-')}</b>", 8, True, RED)])

    sec_tbl = Table(sec_rows, colWidths=[W*0.28, W*0.72])
    sec_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0),(0,-1), ORANGE_PAL),
        ("ROWBACKGROUNDS",(1,0),(-1,-1),[WHITE, ORANGE_XP]),
        ("TOPPADDING",   (0,0),(-1,-1), 6),
        ("BOTTOMPADDING",(0,0),(-1,-1), 6),
        ("LEFTPADDING",  (0,0),(-1,-1), 8),
        ("LINEBELOW",    (0,0),(-1,-2), 0.5, BORDER),
        ("BOX",          (0,0),(-1,-1), 1, BORDER),
        ("LINEAFTER",    (0,0),(0,-1),  2, ORANGE),
    ]))
    story.append(sec_tbl)
    story.append(Spacer(1, 5*mm))

    # ══════════════════════════════════════════════════════════
    # SEKSI 4 — GALERI FOTO BUKTI KONDISI PAKET
    # ══════════════════════════════════════════════════════════
    story.append(P("■  DOKUMENTASI VISUAL KONDISI PAKET", 10, True, ORANGE, space_after=3))
    story.append(Spacer(1, 2*mm))

    if photo_paths:
        trigger_info = target_data.get("evidence_trigger", "Sensor terpicu")
        ev_time      = target_data.get("evidence_time", "-")
        story.append(P(
            f"Kamera IoT mengabadikan {len(photo_paths)} foto secara otomatis. "
            f"Pemicu: <b>{trigger_info}</b> — Waktu: <b>{ev_time}</b>",
            8, False, SLATE, italic=True, space_after=2
        ))
        story.append(Spacer(1, 3*mm))

        # Layout foto: 2 kolom
        PHOTO_W = (W - 6*mm) / 2
        PHOTO_H = PHOTO_W * 0.65

        photo_cells = []
        for i, path in enumerate(photo_paths):
            try:
                img = RLImage(path, width=PHOTO_W, height=PHOTO_H)
                caption = P(f"Foto {i+1} — {trigger_info}", 7, False, MUTED, TA_CENTER, italic=True)
                photo_cells.append([img, caption])
            except Exception as e:
                photo_cells.append([P(f"Foto {i+1} tidak dapat dimuat", 8, False, RED), ""])

        # Kelompokkan 2 per baris
        for row_i in range(0, len(photo_cells), 2):
            pair = photo_cells[row_i:row_i+2]
            if len(pair) == 1:
                pair.append(["", ""])  # kosong kalau ganjil

            row_data = [[
                Table([[pair[0][0]], [pair[0][1]]],
                      colWidths=[PHOTO_W],
                      style=TableStyle([
                          ("BOX",       (0,0),(0,0), 1, BORDER),
                          ("TOPPADDING",(0,1),(0,1), 3),
                          ("ALIGN",     (0,0),(0,1), "CENTER"),
                      ])),
                Table([[pair[1][0]], [pair[1][1]]],
                      colWidths=[PHOTO_W],
                      style=TableStyle([
                          ("BOX",       (0,0),(0,0), 1, BORDER),
                          ("TOPPADDING",(0,1),(0,1), 3),
                          ("ALIGN",     (0,0),(0,1), "CENTER"),
                      ])) if pair[1][0] != "" else P(""),
            ]]
            row_tbl = Table(row_data, colWidths=[PHOTO_W + 3*mm, PHOTO_W + 3*mm])
            row_tbl.setStyle(TableStyle([
                ("VALIGN",      (0,0),(-1,-1), "TOP"),
                ("LEFTPADDING", (1,0),(1,0),   6),
                ("BOTTOMPADDING",(0,0),(-1,-1), 6),
            ]))
            story.append(row_tbl)
    else:
        # Tidak ada foto
        no_photo = Table([[
            P("📷", 24, False, MUTED, TA_CENTER),
            [
                P("Tidak ada foto bukti kondisi paket", 10, True, MUTED),
                Spacer(1, 2*mm),
                P("Kamera IoT tidak menangkap foto selama proses pengiriman berlangsung. "
                  "Ini menunjukkan tidak ada kejadian benturan atau pembukaan paksa yang terdeteksi.",
                  8, False, MUTED, italic=True),
            ]
        ]], colWidths=[18*mm, W-18*mm])
        no_photo.setStyle(TableStyle([
            ("VALIGN",       (0,0),(-1,-1), "MIDDLE"),
            ("BACKGROUND",   (0,0),(-1,-1), LIGHT_BG),
            ("BOX",          (0,0),(-1,-1), 1, BORDER),
            ("TOPPADDING",   (0,0),(-1,-1), 16),
            ("BOTTOMPADDING",(0,0),(-1,-1), 16),
            ("LEFTPADDING",  (0,0),(-1,-1), 12),
        ]))
        story.append(no_photo)

    story.append(Spacer(1, 6*mm))

    # ══════════════════════════════════════════════════════════
    # FOOTER — Pernyataan + Tanda Tangan
    # ══════════════════════════════════════════════════════════
    story.append(Table([[""]], colWidths=[W],
        style=TableStyle([("BACKGROUND",(0,0),(0,0), BORDER),
                          ("ROWHEIGHT",(0,0),(0,0), 1)])))
    story.append(Spacer(1, 4*mm))

    legal = (
        "<b>PERNYATAAN RESMI:</b> Dokumen laporan pengiriman ini dihasilkan secara otomatis oleh sistem "
        "manajemen kargo berharga PT Pos Indonesia (Persero). Seluruh data telemetri GPS, status keamanan "
        "enclosure, dan dokumentasi visual yang tercantum merupakan rekaman asli dari perangkat IoT lapangan "
        "dan dapat dijadikan sebagai bukti pengiriman yang sah secara hukum."
    )
    footer_data = [[
        P(legal, 7.5, False, SLATE, TA_JUSTIFY),
        [
            P(f"Bandung, {tanggal}", 8, False, MUTED, TA_CENTER),
            Spacer(1, 10*mm),
            HRFlowable(width="75%", thickness=1.5, color=ORANGE, hAlign="CENTER"),
            Spacer(1, 2*mm),
            P("<b>KEPALA OPERASIONAL KARGO</b>", 8, True, DARK, TA_CENTER),
            P("PT Pos Indonesia (Persero)", 7, False, MUTED, TA_CENTER, italic=True),
        ],
    ]]
    footer_tbl = Table(footer_data, colWidths=[W - 55*mm, 55*mm])
    footer_tbl.setStyle(TableStyle([
        ("VALIGN",       (0,0),(-1,-1), "TOP"),
        ("BACKGROUND",   (0,0),(0,-1),  ORANGE_PAL),
        ("BOX",          (0,0),(0,-1),  1, BORDER),
        ("TOPPADDING",   (0,0),(-1,-1), 10),
        ("BOTTOMPADDING",(0,0),(-1,-1), 10),
        ("LEFTPADDING",  (0,0),(0,-1),  10),
        ("LEFTPADDING",  (1,0),(1,-1),  10),
        ("RIGHTPADDING", (1,0),(1,-1),  6),
        ("LINEAFTER",    (0,0),(0,-1),  2.5, ORANGE),
    ]))
    story.append(footer_tbl)
    story.append(Spacer(1, 4*mm))

    # Strip oranye bawah
    story.append(Table([[""]], colWidths=[W],
        style=TableStyle([("BACKGROUND",(0,0),(0,0), ORANGE),
                          ("ROWHEIGHT",(0,0),(0,0), 4)])))
    story.append(Spacer(1, 1*mm))
    story.append(P(
        "© 2026 PT Pos Indonesia (Persero) — Dokumen dihasilkan otomatis oleh Sistem Kargo Berharga IoT",
        7, False, MUTED, TA_CENTER, italic=True
    ))

    doc.build(story)
    pdf_stream.seek(0)
    return send_file(pdf_stream, mimetype="application/pdf", as_attachment=True,
                     download_name=f"LaporanDataPengiriman_Pos_{dev_id}.pdf")


# ==========================================================
# SOCKET.IO
# ==========================================================
@socketio.on('connect')
def handle_connect():
    print("Klien websocket tersambung.")
    socketio.emit('ui_refresh', {**current_telemetry, **safety_status})
    socketio.emit('evidence_update', {"photos": captured_evidence, **last_evidence_meta})


# ==========================================================
# ENTRY POINT
# ==========================================================
if __name__ == '__main__':
    socketio.start_background_task(target=check_device_heartbeat)
    start_mqtt_background()
    socketio.run(app, debug=True, port=5001, use_reloader=False)