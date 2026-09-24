#!/usr/bin/env python3

import asyncio
import json
import re
import signal
import subprocess
import threading
import time
from typing import Any, Optional

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
import paho.mqtt.client as mqtt


# ============================================================
# CONFIGURATION
# แก้ไขค่าตั้งค่าหลักในส่วนนี้
# ============================================================

# ------------------------------------------------------------
# MQTT Broker
# ------------------------------------------------------------

MQTT_HOST = "192.168.100.158"
MQTT_PORT = 1883

# เว้นว่างหาก MQTT Broker ไม่มี Username และ Password
MQTT_USERNAME = ""
MQTT_PASSWORD = ""

# ต้องไม่ซ้ำกับ Gateway หรือ MQTT Client ตัวอื่น
GATEWAY_ID = "2805a55f8b15"
MQTT_CLIENT_ID = f"python-mkgw3-{GATEWAY_ID}"

# Topic ที่ใช้ Publish ข้อมูล
MQTT_PUBLISH_TOPIC = f"/MKGW3/{GATEWAY_ID}/send"

MQTT_KEEPALIVE = 60
MQTT_QOS = 0
MQTT_RETAIN = False


# ------------------------------------------------------------
# Report interval
# ------------------------------------------------------------

# รวบรวมข้อมูล Beacon แล้วส่ง MQTT ทุก 30 วินาที
REPORT_INTERVAL_SECONDS = 30.0

# ส่งสถานะ Gateway ทุก 60 วินาที
STATUS_INTERVAL_SECONDS = 60.0


# ------------------------------------------------------------
# BLE scanner
# ------------------------------------------------------------

# จำนวน Beacon สูงสุดใน MQTT message เดียว
MAX_RECORDS_PER_MESSAGE = 20

# จำนวน Beacon สูงสุดที่เก็บในหน่วยความจำ
MAX_PENDING_RECORDS = 5000

# MAC + raw_data เดิม จะไม่ถูกบันทึกถี่กว่าค่านี้
# ตั้งเป็น 0 หากต้องการบันทึกทุก Advertisement
MIN_EVENT_INTERVAL_SECONDS = 0.2

# None = รับทุก Beacon ที่ตรวจพบ
# -85 = รับเฉพาะ Beacon ที่มี RSSI ตั้งแต่ -85 dBm ขึ้นไป
MIN_RSSI: Optional[int] = None

# ใช้เมื่อระบบปฏิบัติการไม่รายงานค่า Connectable
DEFAULT_CONNECTABLE = 1

# 0 = UTC
TIMEZONE_OFFSET = 0

# 1 ใช้แทน Wi-Fi ตามรูปแบบ MKGW3
NET_INTERFACE = 1

# active หรือ passive
BLE_SCANNING_MODE = "active"


# ============================================================
# VALIDATE CONFIGURATION
# ============================================================

if not MQTT_HOST:
    raise ValueError("MQTT_HOST cannot be empty")

if not 1 <= MQTT_PORT <= 65535:
    raise ValueError("MQTT_PORT must be between 1 and 65535")

if MQTT_QOS not in {0, 1, 2}:
    raise ValueError("MQTT_QOS must be 0, 1 or 2")

if REPORT_INTERVAL_SECONDS <= 0:
    raise ValueError(
        "REPORT_INTERVAL_SECONDS must be greater than zero"
    )

if STATUS_INTERVAL_SECONDS <= 0:
    raise ValueError(
        "STATUS_INTERVAL_SECONDS must be greater than zero"
    )

if MAX_RECORDS_PER_MESSAGE <= 0:
    raise ValueError(
        "MAX_RECORDS_PER_MESSAGE must be greater than zero"
    )

if MAX_PENDING_RECORDS <= 0:
    raise ValueError(
        "MAX_PENDING_RECORDS must be greater than zero"
    )

if MIN_EVENT_INTERVAL_SECONDS < 0:
    raise ValueError(
        "MIN_EVENT_INTERVAL_SECONDS cannot be negative"
    )

if BLE_SCANNING_MODE not in {"active", "passive"}:
    raise ValueError(
        "BLE_SCANNING_MODE must be active or passive"
    )


# ============================================================
# GLOBAL STATE
# ============================================================

# ข้อมูล Beacon ที่รอส่ง
pending_records: list[dict[str, Any]] = []

# เวลาล่าสุดที่พบ MAC + raw_data
last_seen: dict[str, float] = {}

# สถานะการเชื่อมต่อ MQTT
mqtt_connected = threading.Event()

# ใช้สำหรับสั่งหยุดโปรแกรม
stop_event: Optional[asyncio.Event] = None

# จำนวนข้อมูลที่ถูกทิ้งเมื่อ Buffer เต็ม
dropped_record_count = 0


# ============================================================
# GENERAL FUNCTIONS
# ============================================================

def unix_timestamp() -> int:
    """
    Unix timestamp หน่วยวินาที
    """

    return int(time.time() * 1000)


def normalize_identifier(address: str) -> str:
    """
    แปลง MAC Address:

    AA:BB:CC:DD:EE:FF
    เป็น:
    aabbccddeeff
    """

    if not address:
        return ""

    return re.sub(
        r"[^0-9a-zA-Z]",
        "",
        address
    ).lower()


def signed_byte(value: int) -> bytes:
    """
    แปลงเลข signed integer เป็น 1 byte
    """

    value = max(-128, min(127, int(value)))

    return value.to_bytes(
        length=1,
        byteorder="little",
        signed=True
    )


def create_ad_structure(
    ad_type: int,
    payload: bytes
) -> bytes:
    """
    สร้าง BLE Advertising Data Structure:

    length | AD type | payload
    """

    if not payload:
        return b""

    structure_length = len(payload) + 1

    if structure_length > 255:
        return b""

    return (
        bytes([structure_length, ad_type])
        + payload
    )


def bluetooth_uuid_to_bytes(
    uuid_text: str
) -> Optional[bytes]:
    """
    แปลง UUID เป็น byte แบบ little-endian

    รองรับ UUID ขนาด 16, 32 และ 128 บิต
    """

    if not uuid_text:
        return None

    normalized = uuid_text.strip().lower()
    bluetooth_base = "-0000-1000-8000-00805f9b34fb"

    # UUID 16 บิต เช่น ea01
    if re.fullmatch(r"[0-9a-f]{4}", normalized):
        return int(normalized, 16).to_bytes(
            2,
            byteorder="little"
        )

    # UUID 32 บิต
    if re.fullmatch(r"[0-9a-f]{8}", normalized):
        return int(normalized, 16).to_bytes(
            4,
            byteorder="little"
        )

    # Bluetooth Base UUID แบบ 16 บิต
    if (
        len(normalized) == 36
        and normalized.startswith("0000")
        and normalized.endswith(bluetooth_base)
    ):
        try:
            return int(
                normalized[4:8],
                16
            ).to_bytes(
                2,
                byteorder="little"
            )
        except ValueError:
            return None

    # Bluetooth Base UUID แบบ 32 บิต
    if (
        len(normalized) == 36
        and normalized.endswith(bluetooth_base)
    ):
        try:
            return int(
                normalized[0:8],
                16
            ).to_bytes(
                4,
                byteorder="little"
            )
        except ValueError:
            return None

    # UUID 128 บิต
    compact = normalized.replace("-", "")

    if not re.fullmatch(r"[0-9a-f]{32}", compact):
        return None

    try:
        return bytes.fromhex(compact)[::-1]
    except ValueError:
        return None


# ============================================================
# RAW BLE ADVERTISEMENT BUILDER
# ============================================================

def build_raw_advertisement(
    advertisement_data: AdvertisementData
) -> str:
    """
    ประกอบ raw_data จากข้อมูลที่ Bleak เปิดเผย

    หมายเหตุ:
    Bleak ไม่ได้เปิดเผย packet ดิบครบทุกไบต์ในทุกระบบ
    raw_data จึงอาจไม่เหมือน MKGW3 แบบไบต์ต่อไบต์
    """

    raw_parts: list[bytes] = []

    # Flags: General Discoverable + BR/EDR Not Supported
    raw_parts.append(
        create_ad_structure(
            ad_type=0x01,
            payload=b"\x06"
        )
    )

    service_data_uuid_bytes = set()

    # บันทึก UUID ที่มีอยู่ใน Service Data
    for uuid_text in (
        advertisement_data.service_data or {}
    ).keys():
        uuid_bytes = bluetooth_uuid_to_bytes(uuid_text)

        if uuid_bytes is not None:
            service_data_uuid_bytes.add(uuid_bytes)

    service_uuid_16 = bytearray()
    service_uuid_32 = bytearray()
    service_uuid_128 = bytearray()

    # Service UUID
    for uuid_text in advertisement_data.service_uuids or []:
        uuid_bytes = bluetooth_uuid_to_bytes(uuid_text)

        if uuid_bytes is None:
            continue

        # ถ้ามี UUID นี้อยู่ใน Service Data แล้ว ไม่ต้องเพิ่มซ้ำ
        if uuid_bytes in service_data_uuid_bytes:
            continue

        if len(uuid_bytes) == 2:
            service_uuid_16.extend(uuid_bytes)
        elif len(uuid_bytes) == 4:
            service_uuid_32.extend(uuid_bytes)
        elif len(uuid_bytes) == 16:
            service_uuid_128.extend(uuid_bytes)

    if service_uuid_16:
        raw_parts.append(
            create_ad_structure(
                ad_type=0x03,
                payload=bytes(service_uuid_16)
            )
        )

    if service_uuid_32:
        raw_parts.append(
            create_ad_structure(
                ad_type=0x05,
                payload=bytes(service_uuid_32)
            )
        )

    if service_uuid_128:
        raw_parts.append(
            create_ad_structure(
                ad_type=0x07,
                payload=bytes(service_uuid_128)
            )
        )

    # Service Data
    for uuid_text, service_payload in (
        advertisement_data.service_data or {}
    ).items():
        uuid_bytes = bluetooth_uuid_to_bytes(uuid_text)

        if uuid_bytes is None:
            continue

        if len(uuid_bytes) == 2:
            ad_type = 0x16
        elif len(uuid_bytes) == 4:
            ad_type = 0x20
        elif len(uuid_bytes) == 16:
            ad_type = 0x21
        else:
            continue

        raw_parts.append(
            create_ad_structure(
                ad_type=ad_type,
                payload=(
                    uuid_bytes
                    + bytes(service_payload)
                )
            )
        )

    # Manufacturer Specific Data
    for company_id, manufacturer_payload in (
        advertisement_data.manufacturer_data or {}
    ).items():
        try:
            company_bytes = int(company_id).to_bytes(
                2,
                byteorder="little",
                signed=False
            )
        except (TypeError, ValueError, OverflowError):
            continue

        raw_parts.append(
            create_ad_structure(
                ad_type=0xFF,
                payload=(
                    company_bytes
                    + bytes(manufacturer_payload)
                )
            )
        )

    # Tx Power
    if advertisement_data.tx_power is not None:
        raw_parts.append(
            create_ad_structure(
                ad_type=0x0A,
                payload=signed_byte(
                    advertisement_data.tx_power
                )
            )
        )

    # Complete Local Name
    if advertisement_data.local_name:
        encoded_name = advertisement_data.local_name.encode(
            "utf-8",
            errors="ignore"
        )[:253]

        if encoded_name:
            raw_parts.append(
                create_ad_structure(
                    ad_type=0x09,
                    payload=encoded_name
                )
            )

    return b"".join(raw_parts).hex()


# ============================================================
# BLE AD STRUCTURE PARSER
# ============================================================

def parse_ble_ad_structures(
    raw_data: str
) -> list[tuple[int, bytes]]:
    """
    แยก raw_data ออกเป็น BLE AD Structures

    รูปแบบ:
    length | AD type | AD data
    """

    try:
        packet = bytes.fromhex(raw_data)
    except ValueError:
        return []

    structures: list[tuple[int, bytes]] = []
    position = 0

    while position < len(packet):
        structure_length = packet[position]

        if structure_length == 0:
            break

        end_position = position + structure_length + 1

        if end_position > len(packet):
            # Packet ไม่สมบูรณ์
            break

        ad_type = packet[position + 1]
        ad_data = packet[position + 2:end_position]

        structures.append(
            (ad_type, ad_data)
        )

        position = end_position

    return structures


# ============================================================
# BXP TAG DECODER
# ============================================================

def decode_bxp_tag(
    raw_data: str
) -> Optional[dict[str, Any]]:
    """
    ถอดรหัส BXP Tag ที่ใช้ Service UUID 0xEA01
    และ BXP payload เริ่มต้นด้วย 0x80

    รูปแบบที่รองรับได้รับการตรวจสอบจากตัวอย่าง:

    020106
    181601ea800500000000ffbc0110fc20ffffffff0c02000001
    07094d4b20546167
    """

    result: dict[str, Any] = {}
    bxp_payload: Optional[bytes] = None

    for ad_type, ad_data in parse_ble_ad_structures(raw_data):

        # 0x08 = Shortened Local Name
        # 0x09 = Complete Local Name
        if ad_type in (0x08, 0x09):
            result["adv_name"] = ad_data.decode(
                "utf-8",
                errors="replace"
            )
            continue

        # 0x16 = Service Data พร้อม UUID 16 บิต
        if ad_type != 0x16:
            continue

        # UUID 2 bytes + BXP payload อย่างน้อย 21 bytes
        if len(ad_data) < 23:
            continue

        service_uuid = int.from_bytes(
            ad_data[0:2],
            byteorder="little",
            signed=False
        )

        # 01 EA ใน packet คือ UUID 0xEA01
        if service_uuid != 0xEA01:
            continue

        payload = ad_data[2:]

        # รองรับ BXP Tag frame 0x80
        if len(payload) < 21:
            continue

        if payload[0] != 0x80:
            continue

        bxp_payload = payload

    if bxp_payload is None:
        return None

    payload = bxp_payload

    # --------------------------------------------------------
    # BXP payload layout
    # --------------------------------------------------------
    #
    # Index 0       : Frame type 0x80
    # Index 1       : Sensor/status flags
    # Index 2-3     : Hall trigger count
    # Index 4-5     : Motion trigger count
    # Index 6-7     : X-axis, signed big-endian
    # Index 8-9     : Y-axis, signed big-endian
    # Index 10-11   : Z-axis, signed big-endian
    # Index 12-15   : Optional sensor/reserved fields
    # Index 16-17   : Battery voltage, mV
    # Index 18-20   : Tag ID
    # --------------------------------------------------------

    sensor_flags = payload[1]

    hall_trigger_count = int.from_bytes(
        payload[2:4],
        byteorder="big",
        signed=False
    )

    motion_trigger_count = int.from_bytes(
        payload[4:6],
        byteorder="big",
        signed=False
    )

    x_axis = int.from_bytes(
        payload[6:8],
        byteorder="big",
        signed=True
    )

    y_axis = int.from_bytes(
        payload[8:10],
        byteorder="big",
        signed=True
    )

    z_axis = int.from_bytes(
        payload[10:12],
        byteorder="big",
        signed=True
    )

    battery_mv = int.from_bytes(
        payload[16:18],
        byteorder="big",
        signed=False
    )

    tag_id = payload[18:21].hex()

    # การแปล Sensor Flags สำหรับ frame ตัวอย่าง 0x80/0x05
    hall_sensor_status = (
        1 if sensor_flags & 0x01 else 0
    )

    accelerometer_check_move = (
        1 if sensor_flags & 0x02 else 0
    )

    have_accelerometer_sensor = (
        1 if sensor_flags & 0x04 else 0
    )

    have_temperature_sensor = (
        1 if sensor_flags & 0x08 else 0
    )

    have_humidity_sensor = (
        1 if sensor_flags & 0x10 else 0
    )

    have_flash = (
        1 if sensor_flags & 0x20 else 0
    )

    result.update({
        "type_code": 8,
        "type": "bxp-tag",
        "hall_sensor_status": hall_sensor_status,
        "have_accelerometer_sensor": (
            have_accelerometer_sensor
        ),
        "accelerometer_check_move": (
            accelerometer_check_move
        ),

        # สะกดตามรูปแบบ JSON ของ MKGW3
        "have_tempersture_sensor": (
            have_temperature_sensor
        ),

        "have_humidity_sensor": have_humidity_sensor,
        "have_flash": have_flash,
        "hall_trigger_count": hall_trigger_count,
        "motion_trigger_count": motion_trigger_count,
        "x_axis_data": x_axis,
        "y_axis_data": y_axis,
        "z_axis_data": z_axis,
        "batt_vol": battery_mv,
        "tagid": tag_id
    })

    return result


# ============================================================
# CONNECTABLE
# ============================================================

def get_connectable(
    device: BLEDevice,
    advertisement_data: AdvertisementData
) -> int:
    """
    พยายามอ่านค่า Connectable จาก platform_data

    หากอ่านไม่ได้จะใช้ DEFAULT_CONNECTABLE
    """

    platform_data = getattr(
        advertisement_data,
        "platform_data",
        None
    )

    if platform_data:
        platform_text = str(platform_data).lower()

        match = re.search(
            r"connectable[^a-z]*(true|false)",
            platform_text
        )

        if match:
            return 1 if match.group(1) == "true" else 0

    return 1 if DEFAULT_CONNECTABLE else 0


# ============================================================
# BUFFER MANAGEMENT
# ============================================================

def cleanup_last_seen(
    current_time: float
) -> None:
    """
    ลบข้อมูลกรองรายการซ้ำที่เก่าแล้ว
    เพื่อป้องกันหน่วยความจำโตต่อเนื่อง
    """

    if len(last_seen) < 10000:
        return

    expiry_seconds = max(
        REPORT_INTERVAL_SECONDS * 2,
        120.0
    )

    expired_keys = [
        key
        for key, seen_time in last_seen.items()
        if current_time - seen_time > expiry_seconds
    ]

    for key in expired_keys:
        last_seen.pop(key, None)


def add_pending_record(
    record: dict[str, Any]
) -> None:
    """
    เพิ่มข้อมูล Beacon ลงใน Buffer
    """

    global dropped_record_count

    if len(pending_records) >= MAX_PENDING_RECORDS:
        # ลบรายการเก่าที่สุด
        pending_records.pop(0)
        dropped_record_count += 1

        if dropped_record_count % 100 == 1:
            print(
                "BLE buffer full; "
                f"dropped records={dropped_record_count}"
            )

    pending_records.append(record)


# ============================================================
# BLE CALLBACK
# ============================================================

def advertisement_callback(
    device: BLEDevice,
    advertisement_data: AdvertisementData
) -> None:
    """
    ทำงานทุกครั้งที่ตรวจพบ BLE Advertisement
    """

    try:
        rssi = int(advertisement_data.rssi)

        # กรอง RSSI
        if MIN_RSSI is not None and rssi < MIN_RSSI:
            return

        beacon_mac = normalize_identifier(
            device.address
        )

        if not beacon_mac:
            return

        raw_data = build_raw_advertisement(
            advertisement_data
        )

        if not raw_data:
            return

        current_monotonic = time.monotonic()

        # ใช้ MAC + raw_data เป็นกุญแจกรองรายการซ้ำ
        event_key = f"{beacon_mac}:{raw_data}"

        previous_monotonic = last_seen.get(
            event_key,
            0.0
        )

        if (
            MIN_EVENT_INTERVAL_SECONDS > 0
            and current_monotonic - previous_monotonic
            < MIN_EVENT_INTERVAL_SECONDS
        ):
            return

        last_seen[event_key] = current_monotonic
        cleanup_last_seen(current_monotonic)

        # พยายามถอดรหัส BXP Tag
        bxp_data = decode_bxp_tag(raw_data)

        if bxp_data is not None:
            type_code = 8
            beacon_type = "bxp-tag"
        else:
            type_code = 10
            beacon_type = "other"

        record: dict[str, Any] = {
            "timestamp": unix_timestamp(),
            "timezone": TIMEZONE_OFFSET,
            "type_code": type_code,
            "type": beacon_type,
            "rssi": rssi,
            "connectable": get_connectable(
                device,
                advertisement_data
            ),
            "mac": beacon_mac,
            "raw_data": raw_data
        }

        # เพิ่มข้อมูลที่ถอดรหัสจาก BXP Tag
        if bxp_data is not None:
            record.update(bxp_data)

        add_pending_record(record)

        if bxp_data is not None:
            battery_mv = bxp_data["batt_vol"]
            battery_v = battery_mv / 1000.0

            print(
                f"BXP Tag "
                f"mac={beacon_mac} "
                f"rssi={rssi} dBm "
                f"battery={battery_mv} mV "
                f"({battery_v:.3f} V) "
                f"tagid={bxp_data['tagid']} "
                f"buffer={len(pending_records)}"
            )
        else:
            print(
                f"BLE "
                f"mac={beacon_mac} "
                f"rssi={rssi} dBm "
                f"type=other "
                f"buffer={len(pending_records)}"
            )

    except Exception as error:
        # ป้องกัน packet รายการเดียวทำให้ Scanner หยุด
        print(
            f"BLE advertisement error: {error}"
        )


# ============================================================
# MQTT CALLBACKS
# ============================================================

def on_mqtt_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties
) -> None:
    """
    ทำงานเมื่อ MQTT เชื่อมต่อสำเร็จหรือไม่สำเร็จ
    """

    if reason_code == 0:
        mqtt_connected.set()

        print(
            f"MQTT connected: "
            f"{MQTT_HOST}:{MQTT_PORT}"
        )

        print(
            f"MQTT publish topic: "
            f"{MQTT_PUBLISH_TOPIC}"
        )
    else:
        mqtt_connected.clear()

        print(
            f"MQTT connection failed: "
            f"{reason_code}"
        )


def on_mqtt_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties
) -> None:
    """
    ทำงานเมื่อ MQTT หลุด
    """

    mqtt_connected.clear()

    print(
        f"MQTT disconnected: {reason_code}"
    )


def on_mqtt_publish(
    client,
    userdata,
    mid,
    reason_code,
    properties
) -> None:
    """
    ทำงานเมื่อส่ง MQTT message แล้ว
    """

    pass


# ============================================================
# MQTT CLIENT
# ============================================================

mqtt_client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=MQTT_CLIENT_ID,
    protocol=mqtt.MQTTv311
)

mqtt_client.on_connect = on_mqtt_connect
mqtt_client.on_disconnect = on_mqtt_disconnect
mqtt_client.on_publish = on_mqtt_publish

mqtt_client.reconnect_delay_set(
    min_delay=1,
    max_delay=30
)

if MQTT_USERNAME:
    mqtt_client.username_pw_set(
        username=MQTT_USERNAME,
        password=MQTT_PASSWORD
    )


# ============================================================
# MQTT PUBLISH
# ============================================================

def publish_json(
    payload: dict[str, Any]
) -> bool:
    """
    แปลง Dictionary เป็น JSON แล้ว Publish ไป MQTT
    """

    if not mqtt_connected.is_set():
        print(
            "MQTT is not connected; "
            "data remains in buffer"
        )
        return False

    try:
        message = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":")
        )

        publish_info = mqtt_client.publish(
            topic=MQTT_PUBLISH_TOPIC,
            payload=message,
            qos=MQTT_QOS,
            retain=MQTT_RETAIN
        )

        if publish_info.rc != mqtt.MQTT_ERR_SUCCESS:
            print(
                f"MQTT publish error: "
                f"code={publish_info.rc}"
            )
            return False

        return True

    except Exception as error:
        print(
            f"MQTT publish exception: {error}"
        )
        return False


def publish_next_scan_batch() -> bool:
    """
    ส่ง Beacon ไม่เกิน MAX_RECORDS_PER_MESSAGE
    ต่อหนึ่ง MQTT message
    """

    if not pending_records:
        return True

    batch_size = min(
        len(pending_records),
        MAX_RECORDS_PER_MESSAGE
    )

    records_to_send = pending_records[:batch_size]

    payload = {
        "msg_id": 3070,
        "device_info": {
            "mac": GATEWAY_ID
        },
        "data": records_to_send
    }

    if not publish_json(payload):
        return False

    # ลบออกจาก Buffer เมื่อ MQTT รับเข้าคิวแล้ว
    del pending_records[:batch_size]

    print(
        f"Published msg_id=3070 "
        f"records={batch_size} "
        f"remaining={len(pending_records)}"
    )

    return True


async def publish_all_pending_records() -> None:
    """
    ส่งข้อมูล Beacon ที่สะสมทั้งหมด
    โดยแบ่งเป็นหลาย MQTT messages
    """

    if not pending_records:
        print(
            "Report interval reached: no BLE data"
        )
        return

    print(
        f"Report interval reached: "
        f"records={len(pending_records)}"
    )

    while pending_records:
        if not mqtt_connected.is_set():
            print(
                "MQTT unavailable; "
                f"records retained={len(pending_records)}"
            )
            break

        if not publish_next_scan_batch():
            break

        await asyncio.sleep(0)


# ============================================================
# REPORT INTERVAL LOOP
# ============================================================

async def scan_report_loop() -> None:
    """
    ส่งรายงาน Beacon ตาม REPORT_INTERVAL_SECONDS
    ค่าเริ่มต้นคือ 30 วินาที
    """

    assert stop_event is not None

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=REPORT_INTERVAL_SECONDS
            )

        except asyncio.TimeoutError:
            await publish_all_pending_records()


# ============================================================
# WI-FI RSSI
# ============================================================

def read_linux_wifi_rssi() -> int:
    """
    อ่านค่า Wi-Fi RSSI บน Linux

    หากอ่านไม่ได้จะคืนค่า 0
    """

    # วิธีที่ 1: คำสั่ง iw
    try:
        result = subprocess.run(
            ["iw", "dev"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False
        )

        interface_name = None

        for line in result.stdout.splitlines():
            stripped = line.strip()

            if stripped.startswith("Interface "):
                interface_name = stripped.split(
                    None,
                    1
                )[1]
                break

        if interface_name:
            link_result = subprocess.run(
                [
                    "iw",
                    "dev",
                    interface_name,
                    "link"
                ],
                capture_output=True,
                text=True,
                timeout=3,
                check=False
            )

            for line in link_result.stdout.splitlines():
                match = re.search(
                    r"signal:\s*(-?\d+)\s*dBm",
                    line
                )

                if match:
                    return int(match.group(1))

    except (
        OSError,
        ValueError,
        subprocess.TimeoutExpired
    ):
        pass

    # วิธีที่ 2: คำสั่ง iwconfig
    try:
        result = subprocess.run(
            ["iwconfig"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False
        )

        match = re.search(
            r"Signal level[=:]\s*(-?\d+)\s*dBm",
            result.stdout
        )

        if match:
            return int(match.group(1))

    except (
        OSError,
        ValueError,
        subprocess.TimeoutExpired
    ):
        pass

    return 0


# ============================================================
# GATEWAY STATUS
# ============================================================

def publish_gateway_status() -> bool:
    """
    ส่งสถานะ Gateway รูปแบบ msg_id 3004
    """

    payload = {
        "msg_id": 3004,
        "device_info": {
            "mac": GATEWAY_ID
        },
        "data": {
            "timestamp": unix_timestamp(),
            "timezone": TIMEZONE_OFFSET,
            "net_interface": NET_INTERFACE,
            "wifi_rssi": read_linux_wifi_rssi()
        }
    }

    if publish_json(payload):
        print(
            "Published msg_id=3004 gateway status"
        )
        return True

    return False


async def gateway_status_loop() -> None:
    """
    ส่งสถานะ Gateway ตาม STATUS_INTERVAL_SECONDS
    """

    assert stop_event is not None

    # รอ MQTT เชื่อมต่อครั้งแรก
    try:
        await asyncio.wait_for(
            stop_event.wait(),
            timeout=2.0
        )
        return

    except asyncio.TimeoutError:
        pass

    while not stop_event.is_set():
        publish_gateway_status()

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=STATUS_INTERVAL_SECONDS
            )

        except asyncio.TimeoutError:
            pass


# ============================================================
# SHUTDOWN
# ============================================================

def request_shutdown() -> None:
    """
    สั่งหยุดโปรแกรม
    """

    if stop_event is not None:
        stop_event.set()


# ============================================================
# MAIN
# ============================================================

async def main() -> None:
    global stop_event

    stop_event = asyncio.Event()
    event_loop = asyncio.get_running_loop()

    # รองรับ Ctrl+C และ systemd stop
    try:
        event_loop.add_signal_handler(
            signal.SIGINT,
            request_shutdown
        )

        event_loop.add_signal_handler(
            signal.SIGTERM,
            request_shutdown
        )

    except (NotImplementedError, RuntimeError):
        pass

    print("=" * 60)
    print("Python MKGW3-compatible BLE to MQTT Gateway")
    print("=" * 60)
    print(f"Gateway ID       : {GATEWAY_ID}")
    print(f"MQTT Broker      : {MQTT_HOST}:{MQTT_PORT}")
    print(f"MQTT Topic       : {MQTT_PUBLISH_TOPIC}")
    print(f"MQTT QoS         : {MQTT_QOS}")
    print(
        f"Report Interval  : "
        f"{REPORT_INTERVAL_SECONDS} seconds"
    )
    print(
        f"Status Interval  : "
        f"{STATUS_INTERVAL_SECONDS} seconds"
    )
    print(f"Minimum RSSI     : {MIN_RSSI}")
    print(f"BLE Scan Mode    : {BLE_SCANNING_MODE}")
    print("=" * 60)

    # เริ่ม MQTT
    mqtt_client.connect_async(
        host=MQTT_HOST,
        port=MQTT_PORT,
        keepalive=MQTT_KEEPALIVE
    )

    mqtt_client.loop_start()

    # สร้าง BLE Scanner
    scanner = BleakScanner(
        detection_callback=advertisement_callback,
        scanning_mode=BLE_SCANNING_MODE
    )

    report_task = asyncio.create_task(
        scan_report_loop()
    )

    status_task = asyncio.create_task(
        gateway_status_loop()
    )

    scanner_started = False

    try:
        await scanner.start()
        scanner_started = True

        print("BLE scanner started")
        print("Press Ctrl+C to stop")

        await stop_event.wait()

    except KeyboardInterrupt:
        request_shutdown()

    except Exception as error:
        print(f"Gateway error: {error}")
        request_shutdown()

    finally:
        print("Stopping gateway...")

        if scanner_started:
            try:
                await scanner.stop()
                print("BLE scanner stopped")

            except Exception as error:
                print(
                    f"BLE scanner stop error: {error}"
                )

        # ส่งข้อมูลที่เหลือก่อนหยุด
        if pending_records and mqtt_connected.is_set():
            print(
                f"Publishing final records: "
                f"{len(pending_records)}"
            )

            await publish_all_pending_records()

            # ให้ MQTT มีเวลาส่งข้อมูล
            await asyncio.sleep(1)

        for task in (report_task, status_task):
            task.cancel()

        await asyncio.gather(
            report_task,
            status_task,
            return_exceptions=True
        )

        try:
            mqtt_client.disconnect()
        except Exception:
            pass

        mqtt_client.loop_stop()

        print(
            f"Gateway stopped; "
            f"unsent records={len(pending_records)}"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        pass
