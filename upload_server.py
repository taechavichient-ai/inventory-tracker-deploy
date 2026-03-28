"""
upload_server.py
เซิร์ฟเวอร์สำหรับรับอัพโหลดไฟล์ Excel (TR / TO / GR) ผ่าน drag-and-drop
รองรับ multi-user, deduplication อัตโนมัติ, cloud deployment

วิธีใช้ (local):
  pip install flask
  python upload_server.py

วิธีใช้ (cloud / Railway / Render):
  ตั้ง environment variable:
    DATA_DIR=/data          ← persistent volume path
    PORT=8080               ← optional, default 5000
    SECRET_KEY=xxx          ← optional

  แล้วรัน: python upload_server.py
"""
import os, subprocess, threading, json
from datetime import datetime
from flask import Flask, request, jsonify, send_file, render_template_string

app = Flask(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────
# On cloud: set DATA_DIR env to a persistent volume (e.g. /data on Railway)
# On local: defaults to the mnt folder beside this script
_HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.environ.get('DATA_DIR', os.path.join(_HERE, 'mnt', 'ตรวจสอบสินค้าจัดส่งสาขา'))
DB_DIR    = os.path.join(DATA_DIR, 'database')
REPORT_HTML = os.path.join(DATA_DIR, 'รายงานค้นหาสินค้า.html')
REPORT_XLSX = os.path.join(DATA_DIR, 'ตารางเปรียบเทียบการสั่ง-ส่ง-รับ.xlsx')

FILE_MAP = {
    'tr': 'รายละเอียดการร้องขอโอนวัตถุดิบ.xlsx',
    'to': 'รายละเอียดการโอนวัตถุดิบ.xlsx',
    'gr': 'รายละเอียดการรับโอนวัตถุดิบ.xlsx',
}

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DB_DIR,   exist_ok=True)

_lock = threading.Lock()   # prevent concurrent pipeline runs

# ── DB size helper ──────────────────────────────────────────────────────────
def db_stats():
    import csv
    stats = {}
    for key in ['tr', 'to', 'gr']:
        path = os.path.join(DB_DIR, f'{key}_database.csv')
        if os.path.exists(path):
            with open(path, encoding='utf-8-sig') as f:
                rows = sum(1 for _ in csv.reader(f)) - 1   # minus header
            size_kb = round(os.path.getsize(path) / 1024, 1)
            stats[key.upper()] = {'rows': rows, 'size_kb': size_kb}
        else:
            stats[key.upper()] = {'rows': 0, 'size_kb': 0}
    return stats

# ================================================================
#  HTML UPLOAD PAGE
# ================================================================
UPLOAD_PAGE = r"""<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>อัพโหลดไฟล์ — ตรวจสอบสินค้า</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',Arial,sans-serif;background:#f0f2f5;color:#222;min-height:100vh}
.topbar{background:#1f4e79;color:#fff;padding:14px 24px;display:flex;align-items:center;gap:12px}
.topbar h1{font-size:17px;font-weight:700}
.topbar small{font-size:12px;opacity:.7;margin-left:auto}
.container{max-width:880px;margin:28px auto;padding:0 16px}
.card{background:#fff;border-radius:10px;box-shadow:0 2px 10px rgba(0,0,0,.08);padding:26px 30px;margin-bottom:20px}
.card h2{font-size:15px;font-weight:700;color:#1f4e79;margin-bottom:6px}
.card p{font-size:13px;color:#555;line-height:1.65;margin-bottom:16px}
.zones{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}
.zone{border:2.5px dashed #b0c4de;border-radius:8px;padding:20px 12px;text-align:center;
      cursor:pointer;transition:.2s;position:relative;min-height:118px;
      display:flex;flex-direction:column;align-items:center;justify-content:center;gap:5px}
.zone:hover,.zone.over{border-color:#1f4e79;background:#eef4ff}
.zone.has-file{border-color:#27ae60;background:#f0fff4;border-style:solid}
.zone-icon{font-size:26px}
.zone-label{font-size:13px;font-weight:700;color:#1f4e79}
.zone-sub{font-size:10px;color:#999;margin-top:1px}
.zone-name{font-size:11px;color:#27ae60;font-weight:600;margin-top:4px;word-break:break-all}
.zone input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}
.btn-run{display:block;width:100%;padding:13px;background:#1f4e79;color:#fff;border:none;
         border-radius:8px;font-size:15px;font-weight:700;cursor:pointer;margin-top:10px;transition:.15s}
.btn-run:hover{background:#16375a}
.btn-run:disabled{background:#9ab0c8;cursor:not-allowed}
.btn-report{display:inline-block;padding:10px 22px;background:#217346;color:#fff;border:none;
            border-radius:8px;font-size:13px;font-weight:700;cursor:pointer;text-decoration:none;transition:.15s;margin-top:8px}
.btn-report:hover{background:#185c38}
.btn-excel{display:inline-block;padding:10px 22px;background:#1f4e79;color:#fff;border:none;
           border-radius:8px;font-size:13px;font-weight:700;cursor:pointer;text-decoration:none;transition:.15s;margin-top:8px;margin-left:8px}
.btn-excel:hover{background:#16375a}
.log-box{background:#1a1a2e;color:#a8d8a8;font-family:monospace;font-size:12px;
         padding:14px;border-radius:8px;max-height:260px;overflow-y:auto;
         white-space:pre-wrap;word-break:break-word;margin-top:14px;display:none}
.log-box.show{display:block}
.status-msg{padding:10px 14px;border-radius:6px;font-size:13px;margin-top:12px;display:none}
.status-msg.show{display:block}
.status-ok{background:#d4edda;color:#155724}
.status-err{background:#f8d7da;color:#721c24}
.status-running{background:#cce5ff;color:#004085}
.db-card{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:4px}
.db-item{background:#f8f9fa;border-radius:8px;padding:12px 16px;text-align:cent}
.db-item .num{font-size:22px;font-weight:700;color:#1f4e79}
.db-item .lbl{font-size:11px;color:#888;margin-top:2px}
.db-item .sz{font-size:10px;color:#aaa}
.steps{display:flex;gap:0;margin:14px 0 4px}
.steps li{flex:1;text-align:center;font-size:11px;padding:5px 2px;border-bottom:3px solid #ddd;color:#aaa;list-style:none;transition:.2s}
.steps li.active{border-color:#1f4e79;color:#1f4e79;font-weight:700}
.steps li.done{border-color:#27ae60;color:#27ae60}
@media(max-width:580px){.zones,.db-card{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="topbar">
  <span style="font-size:22px">📦</span>
  <h1>อัพโหลดไฟล์ข้อมูล ℔ ตรวจส*⸭บสินค้าจัดส่งสาขา</h1>
  <small id="serverTime"></small>
</div>

<div class="container">

  <!-- DB Stats -->
  <div class="card">
    <h2>📊 ฐานข้อมูลปัจจุบัน</h2>
    <p style="margin-bottom:12px">ข้อมูลสะสศÂ�นระบบ — อัพโหลดไฟล์ใหม่ระบบจะ <strong>เพิ่มและตัดซ้ำอัตโนมัติ</strong> ข้อมูลเดิมจะไม่หาย</p>
    <div class="db-card" id="dbStats">
      <div class="db-item"><div class="num">—</div><div class="lbl">TR (ใบร้องขอ)</div><div class="sz">กำลังโหลด...</div></div>
      <div class="db-item"><div class="num">—</div><div class="lbl">TO (ใบโอน)</div><div class="sz"></div></div>
      <div class="db-item"><div class="num">—</div><div class="lbl">GR (ใบรับ)</div><div class="sz"></div></div>
    </div>
  </div>

  <!-- Upload -->
  <div class="card">
    <h2>📤 อัพโหลดไฟล์ใหม่</h2>
    <p>ลากไฟล์วางในช่องที่ตรงกัน หรือคลิกเพื่อเลือก — อัพโหลดได้ทีละ 1, 2 หรือ 3 ไฟล์</p>

    <div class="zones">
      <div class="zone" id="zone-tr" ondragover="onOver(event,'tr')" ondragleave="onLeave('tr')" ondrop="onDrop(event,'tr')">
        <div class="zone-icon">📋</div>
        <div class="zone-label">TR — ใบร้องขอโอน</div>
        <div class="zone-sub">รายละเอียดการร้องขอโอนวัตถุดิบ.xlsx</div>
        <div class="zone-name" id="name-tr"></div>
        <input type="file" accept=".xlsx" onchange="onPick(this,'tr')">
      </div>
      <div class="zone" id="zone-to" ondragover="onOver(event,'to')" ondragleave="onLeave('to')" ondrop="onDrop(event,'to')">
        <div class="zone-icon">🚚</div>
        <div class="zone-label">TO — ใบโอนสินค้า</div>
        <div class="zone-sub">รายละเอียดการโอนวัตถุดิบ.xlsx</div>
        <div class="zone-name" id="name-to"></div>
        <input type="file" accept=".xlsx" onchange="onPick(this,'to')">
      </div>
      <div class="zone" id="zone-gr" ondragover="onOver(event,'gr')" ondragleave="onLeave('gr')" ondrop="onDrop(event,'gr')">
        <div class="zone-icon">✅</div>
        <div class="zone-label">GR — ใบรับโอน</div>
        <div class="zone-sub">รายละเอียดการรับโอนวัตถุดิบ.xlsx</div>
        <div class="zone-name" id="name-gr"></div>
        <input type="file" accept=".xlsx" onchange="onPick(this,'gr')">
      </div>
    </div>

    <ul class="steps" id="stepList">
      <li id="s0">① เลือกไฟล์</li>
      <li id="s1">② บันทึก</li>
      <li id="s2">③ Merge + ตัดซ้ำ</li>
      <li id="s3">④ สร้างตาราง</li>
      <li id="s4">⑤ สร้างรายงาน</li>
      <li id="s5">⑥ เสร็จ</li>
    </ul>

    <button class="btn-run" id="btnRun" onclick="runPipeline()" disabled>🚀 อัพโหลดและประมวลผล</button>
    <div class="status-msg" id="statusMsg"></div>
    <div class="log-box"    id="logBox"></div>
  </div>

  <!-- Report links -->
  <div class="card" id="reportCard" style="display:none">
    <h2>✅ ประมวลผลเสร็จสิ้น</h2>
    <p>รายงานถูกอัพเดทแล้ว คลิกเพื่อเปิด</p>
    <a class="btn-report" href="/report" target="_blank">📊 เปิดรายงาน HTML</a>
    <a class="btn-excel"  href="/excel"  target="_blank">📥 ดาวน์โหลด Excel</a>
  </div>

</div>

<script>
const files = {tr:null, to:null, gr:null};

document.getElementById('serverTime').textContent = new Date().toLocaleString('th-TH');
loadStats();

async function loadStats() {
  try {
    const r = await fetch('/stats');
    const d = await r.json();
    const el = document.getElementById('dbStats');
    const keys = ['TR','TO','GR'];
    const labels = ['TR (ใบร้องขอ)','TO (ใบโอน)','GR (ใบรับ)'];
    el.innerHTML = keys.map((k,i) => `
      <div class="db-item">
        <div class="num">${(d[k]?.rows||0).toLocaleString()}</div>
        <div class="lbl">${labels[i]}</div>
        <div class="sz">${d[k]?.size_kb||0} KB</div>
      </div>`).join('');
  } catch(e) {}
}

function onOver(e,K){e.preventDefault();document.getElementById('zone-'+k).classList.add('over')}
function onLeave(k){document.getElementById('zone-'+k).classList.remove('over')}
function onDrop(e,k){e.preventDefault();onLeave(k);const f=e.dataTransfer.files[0];if(f)setFile(k,f)}
function onPick(el,k){if(el.files[0])setFile(k,el.files[0])}
function setFile(k,f){
  if(!f.name.endsWith('.xlsx')){alert('กรุณาเลือกไฟล์ .xlsx เท่านั้น');return}
  files[k]=f;
  document.getElementById('zone-'+k).classList.add('has-file');
  document.getElementById('name-'+k).textContent='📎 '+f.name;
  document.getElementById('btnRun').disabled=!(files.tr||files.to||files.gr);
}
function setStep(n){
  for(let i=0;i<=5;i++){
    const el=document.getElementById('s'+i);
    el.classList.remove('active','done');
    if(i<n)el.classList.add('done');
    else if(i===n)el.classList.add('active');
  }
}
function showStatus(msg,type){
  const el=document.getElementById('statusMsg');
  el.textContent=msg; el.className='status-msg show status-'+type;
}
function appendLog(t){
  const b=document.getElementById('logBox');
  b.classList.add('show'); b.textContent+=t+'\n'; b.scrollTop=b.scrollHeight;
}
async function runPipeline(){
  const btn=document.getElementById('btnRun');
  btn.disabled=true; btn.textContent='⏳ กำลังประมวลผล...';
  document.getElementById('logBox').textContent='';
  document.getElementById('reportCard').style.display='none';
  setStep(1); showStatus('กำลังอัพโหลดและประมวลผล...','running');
  const fd=new FormData();
  if(files.tr)fd.append('tr',files.tr);
  if(files.to)fd.append('to',files.to);
  if(files.gr)fd.append('gr',files.gr);
  try{
    const res=await fetch('/upload',{method:'POST',body:fd});
    const data=await res.json();
    if(data.log)appendLog(data.log);
    if(!data.success){
      showStatus('❌ '+data.message,'err');
    } else {
      setStep(5);
      const dedupMsg = data.dedup_summary ? '  |  ' + data.dedup_summary : '';
      showStatus('✅ เสร็จสิ้น: '+data.uploaded.join(', ')+dedupMsg,'ok');
      document.getElementById('reportCard').style.display='block';
      loadStats();
    }
  }catch(e){showStatus('❌ ไม่สามารถเชื่อมต่อเซิร์ฟเวอร์: '+e.message,'err')}
  btn.disabled=false; btn.textContent='🚀 อัพโหลดและประมวลผล';
}
</script>
</body>
</html>"""


# ================================================================
#  ROUTES
# ================================================================

@app.route('/')
def index():
    return render_template_string(UPLOAD_PAGE)


@app.route('/stats')
def stats():
    return jsonify(db_stats())


@app.route('/upload', methods=['POST'])
def upload():
    if not _lock.acquire(blocking=False):
        return jsonify({'success': False,
                        'message': 'เซิร์ฟเวอร์กำลังประมวลผลอยู่ กรุณารอสักครู่'})
    log_lines = []

    def run(cmd, label):
        log_lines.append(f'\n── {label} ──')
        env = os.environ.copy()
        env['DATA_DIR'] = DATA_DIR
        result = subprocess.run(cmd, capture_output=True, text=True,
                                cwd=_HERE, env=env)
        log_lines.append(result.stdout.strip())
        if result.returncode != 0:
            log_lines.append('STDERR: ' + result.stderr.strip())
        return result.returncode == 0, result.stderr

    try:
        # 1. Save uploaded files
        uploaded = []
        before_stats = db_stats()
        for key, filename in FILE_MAP.items():
            if key in request.files:
                f = request.files[key]
                if f and f.filename:
                    dest = os.path.join(DATA_DIR, filename)
                    f.save(dest)
                    uploaded.append(key.upper())
                    log_lines.append(f'✔ บันทึก {key.upper()} → {filename}')

        if not uploaded:
            return jsonify({'success': False, 'message': 'ไม่พบไฟล์ที่อัพโหลด'})

        # 2. merge_data.py (deduplication happens here)
        ok, err = run(['python3', 'merge_data.py'], 'merge + dedup')
        if not ok:
            return jsonify({'success': False,
                            'message': 'merge_data.py ผิดพลาด:\n' + err,
                            'log': '\n'.join(log_lines)})

        # 3. build_comparison.py
        ok, err = run(['python3', 'build_comparison.py'], 'build comparison')
        if not ok:
            return jsonify({'success': False,
                            'message': 'build_comparison.py ผิดพลาด:\n' + err,
                            'log': '\n'.join(log_lines)})

        # 4. build_report.py
        ok, err = run(['python3', 'build_report.py'], 'build report')
        if not ok:
            return jsonify({'success': False,
                            'message': 'build_report.py ผิดพลาด:\n' + err,
                            'log': '\n'.join(log_lines)})

        # Dedup summary
        after_stats = db_stats()
        parts = []
        for k in ['TR', 'TO', 'GR']:
            b = before_stats.get(k, {}).get('rows', 0)
            a = after_stats.get(k, {}).get('rows', 0)
            added = a - b
            parts.append(f'{k} +{added:,}' if added > 0 else f'{k} ={a:,}')
        dedup_summary = '  '.join(parts)

        return jsonify({
            'success': True,
            'uploaded': uploaded,
            'message': 'สำเร็จ',
            'dedup_summary': dedup_summary,
            'log': '\n'.join(log_lines),
        })

    finally:
        _lock.release()


@app.route('/report')
def report():
    if os.path.exists(REPORT_HTML):
        return send_file(REPORT_HTML)
    return 'ยังไม่มีรายงาน — กรุณาอัพโหลดไฟล์ก่อน', 404


@app.route('/excel')
def excel():
    if os.path.exists(REPORT_XLSX):
        return send_file(REPORT_XLSX, as_attachment=True)
    return 'ยังไม่มีไฟล์ Excel', 404


# ================================================================
#  MAIN
# ================================================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print('=' * 55)
    print(f'  📦 Upload Server — ตรวจสอบสินค้าจัดส่งสาขา')
    print(f'  DATA_DIR : {DATA_DIR}')
    print(f'  DB_DIR   : {DB_DIR}')
    print(f'  Port     : {port}')
    print(f'  เปิด http://localhost:{port} ในเบราว์เซอร์')
    print('=' * 55)
    app.run(host='0.0.0.0', port=port, debug=False)
