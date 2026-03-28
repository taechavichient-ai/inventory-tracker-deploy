import pandas as pd
import json
import os
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
BASE  = os.environ.get('DATA_DIR',
        os.path.join(_HERE, 'mnt', 'ตรวจสอบสินค้าจัดส่งสาขา')) + os.sep

# Load compiled comparison table
df = pd.read_excel(BASE + 'ตารางเปรียบเทียบการสั่ง-ส่ง-รับ.xlsx', sheet_name='รายละเอียดทั้งหมด', dtype=str)

for col in ['จำนวนสั่ง', 'จำนวนส่ง', 'จำนวนรับ']:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
df['จำนวนเหลือ'] = df['จำนวนส่ง'] - df['จำนวนรับ']

def classify(note):
    n = str(note)
    if '✅' in n: return 'ครบ'
    if 'คลังลืมส่ง' in n or 'ของหมด' in n: return 'คลังลืมส่ง/ของหมด'
    if 'ส่งไม่ครบ' in n: return 'ส่งขาด'
    if 'ของหาย' in n or 'รับไม่ครบ' in n or 'ยังไม่รับ' in n or 'GR' in n:	return 'ไม่ได้รับ/รับขาด'
    if '❌' in n:	return 'ยกเลิก'
    if '⏳' in n: return 'รอดำเนินการ'
    return 'อื่นๆ'

df['หมวดสถานะ'] = df['หมายเหตุ / วิเคราะห์'].apply(classify)

records = []
for _, row in df.iterrows():
    dup_warn = str(row.get('แจ้งเตือนสั่งซ้ำ', '') or '')
    records.append({
        'วันที่สั่ง': str(row.get('วันที่สั่ง (TR)', '') or ''),
        'วันที่ส่ง':  str(row.get('วันที่ส่ง (TO)', '') or ''),
        'วันที่รับ':  str(row.get('วันที่รับ (GR)', '') or ''),
        'ต้นทาง':     str(row.get('ผู้ส่ง', '') or ''),
        'ปลายทาง':    str(row.get('สาขาผู้รับ', '') or ''),
        'รหัส':       str(row.get('รหัสวัตถุดิบ', '') or ''),
        'ชื่อ':        str(row.get('ชื่อสินค้า', '') or ''),
        'หมวดหมู่':   str(row.get('หมวดหมู่', '') or ''),
        'หน่วย':      str(row.get('หน่วย', '') or ''),
        'สั่ง':        row['จำนวนสั่ง'],
        'ส่ง':         row['จำนวนส่ง'],
        'รับ':         row['จำนวนรับ'],
        'เหลือ':       row['จำนวนเหลือ'],
        'หมายเหตุ':   str(row.get('หมายเหตุ / วิเคราะห์', '') or ''),
        'หมวดสถานะ':  row['หมวดสถานะ'],
        'dup':         dup_warn if dup_warn not in ('', 'nan') else '',
        'TR':          str(row.get('เลข TR', '') or ''),
        'TO':          str(row.get('เลข TO', '') or ''),
        'GR':          str(row.get('เลข GR', '') or ''),
    })

branches  = sorted(set(r['ปลายทาง'] for r in records if r['ปลายทาง'] and r['ปลายทาง'] != 'nan'))
categories = sorted(set(r['หมวดหมู่'] for r in records if r['หมวดหมู่'] and r['หมวดหมู่'] != 'nan'))
data_json = json.dumps(records, ensure_ascii=False)
updated_at = datetime.now().strftime('%d/%m/%Y %H:%M')

HTML = f"""<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>รายงานตรวจสอบสินค้าจัดส่งสาขา</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
<style>
  *{{ box-sizing:border-box; margin:0; padding:0; }}
  body{{ font-family:'Segoe UI',Arial,sans-serif; font-size:13px; background:#f0f2f5; color:#222; }}
  .topbar{{ background:#1f4e79; color:#fff; padding:12px 20px; display:flex; align-items:center; justify-content:space-between; }}
  .topbar h1{{ font-size:16px; font-weight:700; }}
  .topbar small{{ font-size:11px; opacity:.75; }}
  .filter-panel{{ background:#fff; padding:14px 20px; border-bottom:1px solid #ddd; display:flex; flex-wrap:wrap; gap:12px; align-items:flex-end; }}
  .fg{{ display:flex; flex-direction:column; gap:4px; }}
  .fg label{{ font-size:11px; font-weight:600; color:#555; text-transform:uppercase; letter-spacing:.5px; }}
  .fg select, .fg input{{ border:1px solid #ccc; border-radius:5px; padding:5px 8px; font-size:13px; min-width:140px; height:32px; }}
  .fg select:focus, .fg input:focus{{ outline:none; border-color:#1f4e79; }}
  .fg.date-row{{ flex-direction:row; align-items:center; gap:6px; }}
  .fg.date-row label{{ margin-bottom:0; white-space:nowrap; }}
  .status-checks{{ display:flex; flex-wrap:wrap; gap:6px; }}
  .status-checks label{{ display:flex; align-items:center; gap:4px; padding:4px 8px; border:1px solid #ddd; border-radius:14px; cursor:pointer; white-space:nowrap; user-select:none; }}
  .status-checks label:hover{{ background:#f5f5f5; }}
  .status-checks input{{ margin:0; cursor:pointer; }}
  .btn-row{{ display:flex; gap:8px; margin-left:auto; align-items:flex-end; }}
  .btn{{ padding:6px 18px; border:none; border-radius:5px; cursor:pointer; font-size:13px; font-weight:600; height:32px; }}
  .btn-search{{ background:#1f4e79; color:#fff; }}
  .btn-search:hover{{ background:#16375a; }}
  .btn-clear{{ background:#e5e7eb; color:#333; }}
  .btn-clear:hover{{ background:#d1d5db; }}
  .btn-export{{ background:#217346; color:#fff; }}
  .btn-export:hover{{ background:#185c38; }}
  .summary-bar{{ background:#e8f0fe; padding:8px 20px; font-size:12px; display:flex; gap:16px; align-items:center; flex-wrap:wrap; border-bottom:1px solid #c5d5e8; }}
  .summary-bar span{{ font-weight:700; }}
  .badge{{ display:inline-block; padding:1px 7px; border-radius:10px; font-size:11px; font-weight:700; }}
  .b-ok{{ background:#d4edda; color:#155724; }}
  .b-warn{{ background:#fff3cd; color:#856404; }}
  .b-danger{{ background:#f8d7da; color:#721c24; }}
  .b-forgot{{ background:#ffe5b4; color:#7a4100; }}
  .b-cancel{{ background:#e2e3e5; color:#383d41; }}
  .b-pending{{ background:#cce5ff; color:#004085; }}
  .b-other{{ background:#e2d9f3; color:#4a235a; }}
  .table-wrap{{ overflow:auto; margin:0; }}
  table{{ width:100%; border-collapse:collapse; font-size:12px; }}
  thead th{{ background:#1f4e79; color:#fff; padding:8px 10px; text-align:left; white-space:nowrap; position:sticky; top:0; z-index:2; cursor:pointer; user-select:none; }}
  thead th:hover{{ background:#16375a; }}
  thead th .sort-icon{{ margin-left:4px; opacity:.5; font-size:10px; }}
  tbody tr:nth-child(even){{ background:#f9f9f9; }}
  tbody tr:hover{{ background:#eef4ff; }}
  td{{ padding:6px 10px; border-bottom:1px solid #e8e8e8; white-space:nowrap; vertical-align:middle; }}
  td.note{{ white-space:normal; max-width:320px; line-height:1.4; }}
  .row-ok{{ background:#f0fff4 !important; }}
  .row-warn{{ background:#fffdf0 !important; }}
  .row-danger{{ background:#fff5f5 !important; }}
  .row-forgot{{ background:#fff4e0 !important; }}
  .row-cancel{{ background:#f5f5f5 !important; color:#888; }}
  .row-pending{{ background:#f0f7ff !important; }}
  .row-dup{{ background:#fff8e1 !important; }}
  .qty-neg{{ color:#c0392b; font-weight:700; }}
  .qty-pos{{ color:#27ae60; }}
  .qty-zero{{ color:#888; }}
  .no-data{{ text-align:center; padding:40px; color:#999; font-size:14px; }}
  .divider{{ width:1px; height:20px; background:#c5d5e8; }}
  @media (max-width:768px){{ .filter-panel{{ flex-direction:column; }} .btn-row{{ margin-left:0; }} }}
</style>
</head>
<body>
<div class="topbar">
  <h1>📦 รายงานตรวจสอบสินค้าจัดส่งสาขา</h1>
  <small>อัปเดตล่าสุด: {updated_at}</small>
</div>

<div class="filter-panel">
  <!-- Date type + range -->
  <div class="fg">
    <label>ประเภทวันที่</label>
    <select id="dateType">
      <option value="วันที่ส่ง">วันที่ส่ง (TO)</option>
      <option value="วันที่สั่ง">วันที่สั่ง (TR)</option>
      <option value="วันที่รับ">วันที่รับ (GR)</option>
    </select>
  </div>
  <div class="fg">
    <label>ตั้งแต่วันที่</label>
    <input type="date" id="dateFrom">
  </div>
  <div class="fg">
    <label>ถึงวันที่</label>
    <input type="date" id="dateTo">
  </div>

  <div class="divider"></div>

  <!-- Branch -->
  <div class="fg">
    <label>สาขาปลายทาง</label>
    <select id="branch">
      <option value="">— ทุกสาขา —</option>
      {''.join(f'<option value="{b}">{b}</option>' for b in branches)}
    </select>
  </div>

  <!-- Category -->
  <div class="fg">
    <label>ป้ายกำกับ</label>
    <select id="category">
      <option value="">— ทุกหมวด —</option>
      {''.join(f'<option value="{c}">{c}</option>' for c in categories)}
    </select>
  </div>

  <div class="divider"></div>

  <!-- Status checkboxes -->
  <div class="fg">
    <label>สถานะ</label>
    <div class="status-checks" id="statusChecks">
      <label><input type="checkbox" value="ครบ" checked> ✅ ครบ</label>
      <label><input type="checkbox" value="ส่งขาด" checked> ⚠️ ส่งขาด</label>
      <label><input type="checkbox" value="ไม่ได้รับ/รับขาด" checked> 📋 ไม่ได้รับ/รับขาด</label>
      <label><input type="checkbox" value="คลังลืมส่ง/ของหมด" checked> 🏭 คลังลืมส่ง/ของหมด</label>
      <label><input type="checkbox" value="ยกเลิก"> ❌ ยกเลิก</label>
      <label><input type="checkbox" value="รอดำเนินการ"> ⏳ รอดำเนินการ</label>
      <label><input type="checkbox" value="อื่นๆ"> 🔍 อื่นๆ (รับเกิน/ส่งเกิน)</label>
    </div>
  </div>

  <!-- Keyword search -->
  <div class="fg" style="grid-column:1/-1">
    <label>🔎 ค้นหา (ชื่อสินค้า / รหัสสินค้า / เลขเอกสาร TR·TO·GR)</label>
    <input type="text" id="keyword" placeholder="พิมพ์คำค้น... กด Enter หรือปุ่มค้นหา" style="width:100%;padding:6px 10px;border:1px solid #c8d8e8;border-radius:6px;font-size:13px;box-sizing:border-box"
      onkeydown="if(event.key==='Enter')applyFilter()">
  </div>

  <!-- Duplicate filter -->
  <div class="fg">
    <label>แจ้งเตือน</label>
    <div class="status-checks">
      <label><input type="checkbox" id="filterDup"> 🔁 แสดงเฉพาะรายการสั่งซ้ำ</label>
    </div>
  </div>

  <div class="btn-row">
    <button class="btn btn-search" onclick="applyFilter()">🔍 ค้นหา</button>
    <button class="btn btn-clear" onclick="clearFilter()">↺ รีเซ็ต</button>
    <button class="btn btn-export" onclick="exportToExcel()" id="btnExport" title="Export ข้อมูลที่แสดงอยู่เป็นไฟล์ Excel">📥 Export Excel</button>
  </div>
</div>

<div class="summary-bar" id="summaryBar">กรุณาเลือกตัวกรองแล้วกด ค้นหา</div>

<div class="table-wrap">
<table id="reportTable">
  <thead>
    <tr>
      <th onclick="sortBy('วันที่สั่ง')">วันที่สั่ง <span class="sort-icon">▲▼</span></th>
      <th onclick="sortBy('วันที่ส่ง')">วันที่ส่ง <span class="sort-icon">▲▼</span></th>
      <th onclick="sortBy('วันที่รับ')">วันที่รับ <span class="sort-icon">▲▼</span></th>
      <th onclick="sortBy('ต้นทาง')">สาขาต้นทาง <span class="sort-icon">▲▼</span></th>
      <th onclick="sortBy('ปลายทาง')">สาขาปลายทาง <span class="sort-icon">▲▼</span></th>
      <th onclick="sortBy('หมวดหมู่')">ป้ายกำกับ <span class="sort-icon">▲▼</span></th>
      <th onclick="sortBy('รหัส')">รหัส <span class="sort-icon">▲▼</span></th>
      <th onclick="sortBy('ชื่อ')">ชื่อสินค้า <span class="sort-icon">▲▼</span></th>
      <th>หน่วย</th>
      <th onclick="sortBy('สั่ง')">จำนวนสั่ง <span class="sort-icon">▲▼</span></th>
      <th onclick="sortBy('ส่ง')">จำนวนส่ง <span class="sort-icon">▲▼</span></th>
      <th onclick="sortBy('รับ')">จำนวนรับ <span class="sort-icon">▲▼</span></th>
      <th onclick="sortBy('เหลือ')">ส่งไม่ได้รับ <span class="sort-icon">▲▼</span></th>
      <th>หมายเหตุ</th>
      <th>แจ้งเตือน</th>
    </tr>
  </thead>
  <tbody id="reportBody">
    <tr><td colspan="15" class="no-data">กรุณาเลือกตัวกรองแล้วกด ค้นหา</td></tr>
  </tbody>
</table>
</div>

<script>
const DATA = {data_json};

let currentSort = {{key: 'วันที่ส่ง', asc: true}};
let filteredData = [];

function parseDate(s) {{
  if (!s || s === 'nan' || s === '') return null;
  // dd/mm/yyyy
  const m = s.match(/^(\\d{{2}})\\/(\\d{{2}})\\/(\\d{{4}})$/);
  if (m) return new Date(+m[3], +m[2]-1, +m[1]);
  return null;
}}

function rowClass(cat) {{
  if (cat === 'ครบ') return 'row-ok';
  if (cat === 'ส่งขาด') return 'row-warn';
  if (cat === 'ไม่ได้รับ/รับขาด') return 'row-danger';
  if (cat === 'คลังลืมส่ง/ของหมด') return 'row-forgot';
  if (cat === 'ยกเลิก') return 'row-cancel';
  if (cat === 'รอดำเนินการ') return 'row-pending';
  return '';
}}

function badge(cat) {{
  const map = {{
    'ครบ': 'b-ok', 'ส่งขาด': 'b-warn',
    'ไม่ได้รับ/รับขาด': 'b-danger', 'คลังลืมส่ง/ของหมด': 'b-forgot',
    'ยกเลิก': 'b-cancel', 'รอดำเนินการ': 'b-pending', 'อื่นๆ': 'b-other'
  }};
  return `<span class="badge ${{map[cat]||'b-other'}}">${{cat}}</span>`;
}}

function fmt(n) {{
  if (n === null || n === undefined || n === '') return '-';
  const v = parseFloat(n);
  if (isNaN(v)) return '-';
  return v % 1 === 0 ? v.toLocaleString() : v.toLocaleString(undefined, {{maximumFractionDigits:2}});
}}

function qtyCell(v, warnIfPos, warnIfNeg) {{
  const n = parseFloat(v);
  if (isNaN(n) || n === 0) return `<td class="qty-zero" style="text-align:right">-</td>`;
  let cls = '';
  if (warnIfNeg && n < 0) cls = 'qty-neg';
  else if (warnIfPos && n > 0) cls = 'qty-warn';
  else if (n > 0) cls = 'qty-pos';
  return `<td class="${{cls}}" style="text-align:right">${{fmt(n)}}</td>`;
}}

function applyFilter() {{
  const dateType = document.getElementById('dateType').value;
  const from = document.getElementById('dateFrom').value ? new Date(document.getElementById('dateFrom').value) : null;
  const to   = document.getElementById('dateTo').value   ? new Date(document.getElementById('dateTo').value)   : null;
  if (to) to.setHours(23,59,59);
  const branch   = document.getElementById('branch').value;
  const category = document.getElementById('category').value;
  const dupOnly  = document.getElementById('filterDup').checked;
  const checked  = Array.from(document.querySelectorAll('#statusChecks input:checked')).map(e=>e.value);
  const kw = (document.getElementById('keyword').value || '').trim().toLowerCase();

  filteredData = DATA.filter(r => {{
    const dKey = dateType;
    const d = parseDate(r[dKey]);
    if (from && (!d || d < from)) return false;
    if (to   && (!d || d > to  )) return false;
    if (branch   && r['ปลายทาง']  !== branch)   return false;
    if (category && r['หมวดหมู่'] !== category) return false;
    if (!checked.includes(r['หมวดสถานะ'])) return false;
    if (dupOnly && !r['dup']) return false;
    if (kw) {{
      const hay = [r['ชื่อ'], r['รหัส'], r['TR'], r['TO'], r['GR']]
                  .map(v => (v||'').toLowerCase()).join(' ');
      if (!hay.includes(kw)) return false;
    }}
    return true;
  }});

  renderTable();
  renderSummary(checked);
}}

function renderTable() {{
  const sorted = [...filteredData].sort((a,b) => {{
    const k = currentSort.key;
    let va = a[k], vb = b[k];
    if (['สั่ง','ส่ง','รับ','เหลือ'].includes(k)) {{
      va = parseFloat(va)||0; vb = parseFloat(vb)||0;
    }} else {{
      // date sort
      if (['วันที่สั่ง','วันที่ส่ง','วันที่รับ'].includes(k)) {{
        va = parseDate(va)||new Date(0); vb = parseDate(vb)||new Date(0);
      }} else {{
        va = (va||'').toString().toLowerCase(); vb = (vb||'').toString().toLowerCase();
      }}
    }}
    if (va < vb) return currentSort.asc ? -1 : 1;
    if (va > vb) return currentSort.asc ? 1 : -1;
    return 0;
  }});

  const tbody = document.getElementById('reportBody');
  if (sorted.length === 0) {{
    tbody.innerHTML = '<tr><td colspan="15" class="no-data">ไม่พบข้อมูลที่ตรงกับเงื่อนไข</td></tr>';
    return;
  }}

  tbody.innerHTML = sorted.map(r => {{
    const note = (r['หมายเหตุ']||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    const ehl = parseFloat(r['เหลือ'])||0;
    const ehlClass = ehl > 0 ? 'qty-neg' : (ehl < 0 ? 'qty-pos' : 'qty-zero');
    const ehlFmt = ehl === 0 ? '-' : fmt(ehl);
    const dupWarn = (r['dup']||'').replace(/&/g,'&amp;');
    const rowCls = r['dup'] ? 'row-dup' : rowClass(r['หมวดสถานะ']);
    return `<tr class="${{rowCls}}">
      <td>${{r['วันที่สั่ง']||'-'}}</td>
      <td>${{r['วันที่ส่ง']||'-'}}</td>
      <td>${{r['วันที่รับ']||'-'}}</td>
      <td>${{r['ต้นทาง']||'-'}}</td>
      <td>${{r['ปลายทาง']||'-'}}</td>
      <td style="font-size:11px;color:#555">${{r['หมวดหมู่']||'-'}}</td>
      <td style="font-size:11px;color:#666">${{r['รหัส']||'-'}}</td>
      <td>${{(r['ชื่อ']||'-').replace(/&/g,'&amp;')}}</td>
      <td style="text-align:center">${{r['หน่วย']||''}}</td>
      <td style="text-align:right">${{fmt(r['สั่ง'])}}</td>
      <td style="text-align:right">${{fmt(r['ส่ง'])}}</td>
      <td style="text-align:right">${{fmt(r['รับ'])}}</td>
      <td style="text-align:right" class="${{ehlClass}}">${{ehlFmt}}</td>
      <td class="note">${{note}}</td>
      <td class="note" style="color:#b45309;font-weight:600">${{dupWarn}}</td>
    </tr>`;
  }}).join('');
}}

function renderSummary(checked) {{
  const total = filteredData.length;
  const counts = {{}};
  filteredData.forEach(r => {{ counts[r['หมวดสถานะ']] = (counts[r['หมวดสถานะ']]||0)+1; }});
  const parts = Object.entries(counts).map(([k,v]) => `${{badge(k)}} ${{v}} รายการ`).join(' &nbsp; ');
  document.getElementById('summaryBar').innerHTML =
    `แสดง <span>${{total.toLocaleString()}}</span> รายการ &nbsp;|&nbsp; ${{parts}}`;
}}

function sortBy(key) {{
  if (currentSort.key === key) currentSort.asc = !currentSort.asc;
  else {{ currentSort.key = key; currentSort.asc = true; }}
  renderTable();
}}

function clearFilter() {{
  document.getElementById('dateFrom').value = '';
  document.getElementById('dateTo').value = '';
  document.getElementById('branch').value = '';
  document.getElementById('category').value = '';
  document.getElementById('keyword').value = '';
  document.getElementById('filterDup').checked = false;
  document.querySelectorAll('#statusChecks input').forEach(e => {{
    e.checked = ['ครบ','ส่งขาด','ไม่ได้รับ/รับขาด','คลังลืมส่ง/ของหมด'].includes(e.value);
  }});
  filteredData = [];
  document.getElementById('reportBody').innerHTML = '<tr><td colspan="15" class="no-data">กรุณาเลือกตัวกรองแล้วกด ค้นหา</td></tr>';
  document.getElementById('summaryBar').innerHTML = 'กรุณาเลือกตัวกรองแล้วกด ค้นหา';
}}

function exportToExcel() {{
  if (filteredData.length === 0) {{
    alert('ไม่มีข้อมูลที่จะ Export\\nกรุณาเลือกตัวกรองและกด ค้นหา ก่อน');
    return;
  }}
  const colDefs = [
    {{ header: 'วันที่สั่ง (TR)',    key: 'วันที่สั่ง',  width: 14 }},
    {{ header: 'วันที่ส่ง (TO)',     key: 'วันที่ส่ง',   width: 14 }},
    {{ header: 'วันที่รับ (GR)',     key: 'วันที่รับ',   width: 14 }},
    {{ header: 'สาขาต้นทาง',        key: 'ต้นทาง',      width: 18 }},
    {{ header: 'สาขาปลายทาง',       key: 'ปลายทาง',     width: 22 }},
    {{ header: 'ป้ายกำกับ',         key: 'หมวดหมู่',    width: 14 }},
    {{ header: 'เลข TR',            key: 'TR',           width: 16 }},
    {{ header: 'เลข TO',            key: 'TO',           width: 16 }},
    {{ header: 'เลข GR',            key: 'GR',           width: 30 }},
    {{ header: 'รหัสสินค้า',        key: 'รหัส',        width: 16 }},
    {{ header: 'ชื่อสินค้า',        key: 'ชื่อ',        width: 30 }},
    {{ header: 'หน่วย',             key: 'หน่วย',       width: 8  }},
    {{ header: 'จำนวนสั่ง',         key: 'สั่ง',        width: 12 }},
    {{ header: 'จำนวนส่ง',          key: 'ส่ง',         width: 12 }},
    {{ header: 'จำนวนรับ',          key: 'รับ',         width: 12 }},
    {{ header: 'ส่งไม่ได้รับ',      key: 'เหลือ',       width: 14 }},
    {{ header: 'สถานะ',             key: 'หมวดสถานะ',   width: 20 }},
    {{ header: 'หมายเหตุ / วิเคราะห์', key: 'หมายเหตุ', width: 55 }},
    {{ header: 'แจ้งเตือนสั่งซ้ำ', key: 'dup',         width: 45 }},
  ];

  // Sort same as current table view
  const sorted = [...filteredData].sort((a,b) => {{
    const k = currentSort.key;
    let va = a[k], vb = b[k];
    if (['สั่ง','ส่ง','รับ','เหลือ'].includes(k)) {{
      va = parseFloat(va)||0; vb = parseFloat(vb)||0;
    }} else if (['วันที่สั่ง','วันที่ส่ง','วันที่รับ'].includes(k)) {{
      va = parseDate(va)||new Date(0); vb = parseDate(vb)||new Date(0);
    }} else {{
      va = (va||'').toString().toLowerCase(); vb = (vb||'').toString().toLowerCase();
    }}
    if (va < vb) return currentSort.asc ? -1 : 1;
    if (va > vb) return currentSort.asc ? 1 : -1;
    return 0;
  }});

  const ws_data = [
    colDefs.map(c => c.header),
    ...sorted.map(r => colDefs.map(c => {{
      const v = r[c.key];
      if (v === null || v === undefined || v === '' || v === 'nan') return '';
      if (['สั่ง','ส่ง','รับ','เหลือ'].includes(c.key)) return isNaN(parseFloat(v)) ? '' : parseFloat(v);
      return String(v);
    }}))
  ];

  const ws = XLSX.utils.aoa_to_sheet(ws_data);

  // Column widths
  ws['!cols'] = colDefs.map(c => ({{ wch: c.width }}));

  // Freeze top row
  ws['!freeze'] = {{ xSplit: 0, ySplit: 1 }};

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'รายงาน');

  const now = new Date();
  const dateStr = now.getFullYear() + '-' +
    String(now.getMonth()+1).padStart(2,'0') + '-' +
    String(now.getDate()).padStart(2,'0');
  XLSX.writeFile(wb, `รายงานสินค้า_${{dateStr}}.xlsx`);
}}

// Auto-fill date range from data on load
(function() {{
  const dates = DATA.map(r => parseDate(r['วันที่ส่ง'])).filter(Boolean);
  if (dates.length) {{
    const mn = new Date(Math.min(...dates)), mx = new Date(Math.max(...dates));
    const fmt2 = d => d.toISOString().slice(0,10);
    document.getElementById('dateFrom').value = fmt2(mn);
    document.getElementById('dateTo').value   = fmt2(mx);
  }}
}})();
</script>
</body>
</html>"""

out_path = BASE + 'รายงานค้นหาสินค้า.html'
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(HTML)

print(f'Saved: {out_path}')
print(f'Total records: {len(records)}')
