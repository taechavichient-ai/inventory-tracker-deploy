"""
merge_data.py
รันเมื่อมีไฟล์ข้อมูลใหม่ — จะ merge เข้า database โดยตัดแถวซ้ำออก

ตัดซ้ำด้วย key:
  TR  → TR number + รหัสวัตถุดิบ
  TO  → TO number + รหัสวัตถุดิบ
  GR  → GR number + TO number + รหัสวัตถุดิบ
"""
import pandas as pd
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
BASE  = os.environ.get('DATA_DIR',
        os.path.join(_HERE, 'mnt', 'ตรวจสอบสินค้าจัดส่งสาขา')) + os.sep
DB    = os.path.join(BASE, 'database') + os.sep
os.makedirs(DB, exist_ok=True)

# ── config ──────────────────────────────────────────────────────────────────
FILES = {
    'tr': {
        'src':  BASE + 'รายละเอียดการร้องขอโอนวัตถุดิบ.xlsx',
        'db':   DB   + 'tr_database.csv',
        'keys': ['TR', 'รหัสวัตถุดิบ'],
    },
    'to': {
        'src':  BASE + 'รายละเอียดการโอนวัตถุดิบ.xlsx',
        'db':   DB   + 'to_database.csv',
        'keys': ['TO', 'รหัสวัตถุดิบ'],
    },
    'gr': {
        'src':  BASE + 'รายละเอียดการรับโอนวัตถุดิบ.xlsx',
        'db':   DB   + 'gr_database.csv',
        'keys': ['GR', 'TO', 'รหัสวัตถุดิบ'],
    },
}

def load_src(path):
    df = pd.read_excel(path, header=1)
    df = df[df.iloc[:, 0] != 'Total'].copy()
    df = df[df.iloc[:, 0].notna()].copy()
    return df

stats = {}
for name, cfg in FILES.items():
    print(f'\n── {name.upper()} ──')
    new_df = load_src(cfg['src'])
    print(f'  ไฟล์ใหม่  : {len(new_df):,} แถว')

    if os.path.exists(cfg['db']):
        old_df = pd.read_csv(cfg['db'], dtype=str)
        print(f'  Database  : {len(old_df):,} แถว')
        combined = pd.concat([old_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=cfg['keys'], keep='last')
        print(f'  หลัง merge : {len(combined):,} แถว')
        added = len(combined) - len(old_df)
        stats[name] = {'before': len(old_df), 'after': len(combined), 'added': added}
    else:
        combined = new_df.drop_duplicates(subset=cfg['keys'], keep='last')
        print(f'  สร้าง database ใหม่ : {len(combined):,} แถว')
        stats[name] = {'before': 0, 'after': len(combined), 'added': len(combined)}

    combined.to_csv(cfg['db'], index=False, encoding='utf-8-sig')
    print(f'  บันทึกแล้ว → {cfg["db"]}')

print('\n' + '='*50)
print('สรุป:')
for name, s in stats.items():
    print(f'  {name.upper()}: {s["before"]:,} → {s["after"]:,} แถว  (+{s["added"]:,} แถวใหม่)')
print('\nเสร็จสิ้น — รัน build_comparison.py ต่อได้เลย')
