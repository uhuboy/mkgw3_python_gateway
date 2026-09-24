# MKGW3 Python Gateway

โปรเจกต์ Python สำหรับเชื่อมต่อและจัดการข้อมูลผ่านเกตเวย์ **MKGW3 (BLE to PoE / WiFi Gateway)** โดยทำหน้าที่รับข้อมูล Advertisement จากอุปกรณ์ Beacon และส่งต่อข้อมูลผ่านโปรโตคอล MQTT

## คุณสมบัติเด่น (Features)
- รองรับการเชื่อมต่อกับเกตเวย์ MKGW3
- แปลงและจัดการข้อมูลจาก Bluetooth Low Energy (BLE)
- ส่งข้อมูลต่อยอดไปยังระบบ Cloud หรือ MQTT Broker
- ตั้งค่าและใช้งานง่ายด้วยภาษา Python

## การติดตั้ง (Installation)

1. Clone โปรเจกต์นี้ลงในเครื่องของคุณ:
   ```bash
   git clone https://github.com/uhuboy/mkgw3_python_gateway.git
   cd mkgw3_python_gateway
   ```

2. ติดตั้งไลบรารีที่จำเป็น:
   ```bash
   pip install -r requirements.txt
   ```

## การใช้งาน (Usage)

1. ตั้งค่าไฟล์คอนฟิกูเรชัน (เช่น ค่า IP ของเกตเวย์ หรือ MQTT Broker) ให้เรียบร้อย
2. รันสคริปต์หลักเพื่อเริ่มใช้งาน:
   ```bash
   python mkgw3_python_gateway.py
   ```

## การสนับสนุน (Support)
หากพบปัญหาหรือต้องการสอบถามข้อมูลเพิ่มเติม สามารถเปิด Issue ได้ที่ GitHub Repository นี้ครับ
