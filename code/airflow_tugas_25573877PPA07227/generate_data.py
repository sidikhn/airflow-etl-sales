import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "data" / "raw" / "sales.csv"
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
products = [("P001","Laptop",7500000),("P002","Mouse",150000),("P003","Keyboard",350000),("P004","Monitor",1800000),("P005","Headset",450000)]
cities=["Yogyakarta","Jakarta","Bandung","Surabaya","Semarang"]
rows=[]
start=date(2025,1,1)
for sale_id in range(1,301):
    pid,pname,price=random.choice(products); qty=random.randint(1,5)
    d=start+timedelta(days=random.randint(0,364))
    rows.append({"sale_id":sale_id,"sale_date":d.isoformat(),"product_id":pid,"product_name":pname,"quantity":qty,"unit_price":price,"total_amount":qty*price,"city":random.choice(cities)})
nullable=["sale_date","product_id","product_name","quantity","unit_price","total_amount","city"]
positions=[(i,c) for i in range(len(rows)) for c in nullable]
for i,c in random.sample(positions,int(len(positions)*0.05)):
    rows[i][c]=None
duplicates=[dict(r) for r in random.sample(rows,15)]
rows.extend(duplicates); random.shuffle(rows)
fields=["sale_id","sale_date","product_id","product_name","quantity","unit_price","total_amount","city"]
with OUTPUT_FILE.open("w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(rows)
print(f"Dataset: {OUTPUT_FILE} | rows={len(rows)} | injected_null_cells=105 | duplicate_rows=15")
