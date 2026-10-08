from pathlib import Path,PurePosixPath
import zipfile,hashlib,csv,io,json,re,stat
import pandas as pd
root=Path(__file__).resolve().parents[1]
archive=root/'ALL SAGEMAKER PREDICTIONS.zip'
target=root/'sagemaker'
secret=re.compile(rb'-----BEGIN (?:[A-Z ]+)?PRIVATE KEY-----|(?:sk_live_|sk-svcacct-|ghp_)[A-Za-z0-9_-]{16,}')
safe=[];excluded=0
with zipfile.ZipFile(archive) as z:
 for i in z.infolist():
  p=PurePosixPath(i.filename)
  if i.is_dir() or '__MACOSX' in p.parts or p.name.startswith('._') or p.name=='.DS_Store':continue
  if p.is_absolute() or '..' in p.parts or stat.S_ISLNK(i.external_attr>>16):raise ValueError('Unsafe archive path')
  b=z.read(i)
  if secret.search(b):excluded+=1;continue
  safe.append((str(PurePosixPath(*p.parts[1:])),b))
# Sanitize only repository copy, never the user's Desktop originals.
with zipfile.ZipFile(archive.with_suffix('.safe.zip'),'w',zipfile.ZIP_DEFLATED) as z:
 for name,b in safe:z.writestr('ALL SAGEMAKER PREDICTIONS/'+name,b)
archive.with_suffix('.safe.zip').replace(archive)
items={}
tickers=['GOOGL','GOOG','NVDA','NFLX','PLTR','AAPL','AMZN','MSFT','META','TSLA','SPY','QQQ','WMT','BTC','XAUUSD','GOLD','AVGO','AMD']
for name,b in safe:
 dest=target/'archive'/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(b)
 if not name.lower().endswith('.csv'):continue
 digest=hashlib.sha256(b).hexdigest();path='sagemaker/archive/'+name
 if digest in items:items[digest]['aliases'].append(path);continue
 base=Path(name).stem
 ticker=next((s for s in tickers if s.lower() in name.lower()),'')
 if ticker=='BTC':ticker='BTC-USD'
 if ticker=='GOLD':ticker='XAUUSD'
 entry=dict(id=digest,path=path,aliases=[],name=base,ticker=ticker,kind='table',frequency='1D',quantiles=[],row_count=0,start_at=None,end_at=None,metrics=None,warnings=[])
 try:
  rows=list(csv.reader(io.StringIO(b.decode('utf-8-sig'))));headers=rows[0];entry['row_count']=len(rows)-1
  qs=[]
  for i,h in enumerate(headers):
   m=re.fullmatch(r'P(\d+(?:\.\d+)?)',h.strip(),re.I)
   if m and 0<float(m[1])<100:qs.append((float(m[1])/100,i))
  ti=next((i for i,h in enumerate(headers) if h.lower().strip() in ['date','datetime','timestamp','time']),None)
  if ti is not None:
   if any(not re.match(r'^\d{4}-\d{2}-\d{2}',r[ti]) for r in rows[1:] if any(r)):raise ValueError('The export needs explicit year-month-day timestamps.')
   stamps=[pd.to_datetime(r[ti],utc=True).isoformat().replace('+00:00','Z') for r in rows[1:] if any(r)]
   if any(pd.Timestamp(stamps[i])<=pd.Timestamp(stamps[i-1]) for i in range(1,len(stamps))):raise ValueError('CSV timestamps must be unique and increasing.')
   if stamps:
    entry.update(start_at=stamps[0],end_at=stamps[-1],row_count=len(stamps))
    delta=(pd.Timestamp(stamps[1])-pd.Timestamp(stamps[0])).total_seconds() if len(stamps)>1 else 86400
    entry['frequency']='1h' if delta==3600 else '1min' if delta==60 else '1D' if delta>=86400 else str(int(delta//60))+'min'
   if qs:
    vals=[[float(r[i]) for _,i in sorted(qs)] for r in rows[1:] if any(r)]
    if any(any(not pd.notna(v) or abs(v)==float('inf') for v in vs) for vs in vals):raise ValueError('Non-finite quantile.')
    if any(any(vs[i]<vs[i-1] for i in range(1,len(vs))) for vs in vals):raise ValueError('Source contains crossed quantiles.')
    entry.update(kind='forecast',quantiles=[q for q,_ in sorted(qs)])
    if any(v<0 for vs in vals for v in vs):entry['warnings'].append('Source includes negative predictions; original values are preserved.')
   elif any(h.strip().lower() in ['price','target','close'] for h in headers):entry['kind']='history'
  if not rows or headers[0].startswith('{\\rtf'):raise ValueError('This export is RTF, not a CSV table.')
 except Exception as e:entry['kind']='file';entry['warnings']=[str(e)[:180]]
 items[digest]=entry
catalog={'schema_version':'sagemaker_catalog_v1','items':list(items.values())}
(target/'catalog.json').write_text(json.dumps(catalog,indent=2)+'\n')
(root/'quantura_site/functions_explore/src/sagemakerCatalog.json').write_text(json.dumps(catalog,indent=2)+'\n')
print(json.dumps({'safe_files':len(safe),'excluded_sensitive_files':excluded,'unique_csvs':len(items),'kinds':{k:sum(e['kind']==k for e in items.values()) for k in ['forecast','history','table','file']}}))
