#!/usr/bin/env python3
import os,sys,time,json,subprocess,sqlite3,hashlib,statistics
from pathlib import Path
root=Path(os.environ.get('ORDER_WORKSPACE',str(Path(__file__).resolve().parent)))
name,scenario,calls,events,interval=sys.argv[1:];calls=int(calls);events=int(events);interval=int(interval)
run=root/'runs'/name;run.mkdir();topic='source_'+name.replace('-','_')
admin=['docker','exec',os.environ.get('ORDER_CONTAINER','kafka-order-fix-20261002'),'/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:9094','--topic',topic]
with (run/'topic.txt').open('w') as f:subprocess.run(admin+['--create','--partitions','3','--replication-factor','1'],stdout=f,stderr=subprocess.STDOUT,check=True)
driver=Path(os.environ.get('ORDER_DRIVER',str(root/'bin/test_ordering_drill')))
command=[str(driver),str(run),topic,scenario,str(calls),str(events),str(interval)]
env=dict(os.environ,LD_LIBRARY_PATH=os.environ.get('ORDER_LIBDIR','/workspace/.cloud-setup/sysroot/usr/lib/x86_64-linux-gnu'))
log=(run/'sender.log').open('w');proc=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
started=time.monotonic();fault_start=None;fault_end=None;rebuild=False;restarted=False;processes=[proc.pid];actions=[];peak_rss=0;peak_disk=0
flag=root/('BLACKHOLE' if scenario=='blackhole' else 'CUT')
try:
 while proc.poll() is None:
  elapsed=time.monotonic()-started
  if elapsed>110:raise RuntimeError('scenario deadline')
  statusfile=run/('status-resume.jsonl' if restarted else 'status.jsonl')
  status={}
  if statusfile.exists():
   lines=statusfile.read_text().splitlines()
   if lines:
    try:status=json.loads(lines[-1])
    except json.JSONDecodeError:pass
  try:
   rss=next(x for x in Path(f'/proc/{proc.pid}/status').read_text().splitlines() if x.startswith('VmRSS:'))
   peak_rss=max(peak_rss,int(rss.split()[1]))
  except (FileNotFoundError,StopIteration):pass
  peak_disk=max(peak_disk,sum(p.stat().st_size for p in run.glob('outbox.db*') if p.is_file()))
  if scenario in ('cut','rebuild','restart','ttl','blackhole') and fault_start is None and status.get('acked',0)>=5:
   flag.write_text('isolated fault\n');fault_start=time.monotonic();actions.append(dict(action='fault_on',time_ms=time.time_ns()//1000000,pid=proc.pid))
  if scenario=='rebuild' and fault_start and not rebuild and time.monotonic()-fault_start>1:
   (run/'rebuild').write_text('request\n');rebuild=True;actions.append(dict(action='request_rebuild',time_ms=time.time_ns()//1000000))
  if scenario=='restart' and fault_start and not restarted and (run/'admission.done').exists():
   db=sqlite3.connect(f'file:{run}/outbox.db?mode=ro',uri=True);state=db.execute('select event_id,call_uuid,call_seq,state from outbox order by call_uuid,call_seq').fetchall();db.close();(run/'before-kill.json').write_text(json.dumps(state,indent=2))
   proc.kill();proc.wait();log.close();actions.append(dict(action='SIGKILL',time_ms=time.time_ns()//1000000,pid=proc.pid,rows=len(state)))
   log=(run/'sender-resume.log').open('w');cmd=command.copy();cmd[3]='resume';proc=subprocess.Popen(cmd,env=env,stdout=log,stderr=subprocess.STDOUT);processes.append(proc.pid);restarted=True;actions.append(dict(action='resume',time_ms=time.time_ns()//1000000,pid=proc.pid))
  hold=5 if scenario=='ttl' else 3.5
  if fault_start and fault_end is None and time.monotonic()-fault_start>hold and (scenario!='restart' or restarted):
   flag.unlink(missing_ok=True);fault_end=time.monotonic();actions.append(dict(action='fault_off',time_ms=time.time_ns()//1000000))
  time.sleep(.02)
 if proc.returncode:raise RuntimeError(f'driver exit {proc.returncode}')
finally:
 flag.unlink(missing_ok=True)
 if proc.poll() is None:proc.kill();proc.wait()
 log.close();(run/'actions.json').write_text(json.dumps(actions,indent=2))
with (run/'readback.log').open('w') as f:subprocess.run([os.environ.get('ORDER_READER',str(root/'bin/order_readback')),topic,str(run/'records.tsv')],env=env,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=90)
plan={(x.split('\t')[0],int(x.split('\t')[1])) for x in (run/'source-plan.tsv').read_text().splitlines()}
admissions={};latency=[]
for line in (run/'admissions.tsv').read_text().splitlines():
 id,call,seq,at,us,ok,error=line.split('\t');assert ok=='1',(line,'admission failed');admissions[id]=(call,int(seq));latency.append(int(us))
assert set(admissions.values())==plan and len(admissions)==len(plan)
records=[];seen=set();orders={};partitions={};allorders={};duplicates=[]
for line in (run/'records.tsv').read_text().splitlines():
 partition,offset,id,header,key,payload=line.split('\t',5);payload=json.loads(payload);identity=(payload['call_id'],int(payload['sequence']));assert identity==admissions[id];assert key==identity[0] and int(header)==identity[1]
 record=dict(partition=int(partition),offset=int(offset),event_id=id,call=identity[0],sequence=identity[1]);records.append(record);partitions.setdefault(identity[0],set()).add(int(partition));allorders.setdefault(identity[0],[]).append(identity[1])
 if id in seen:duplicates.append(record);continue
 seen.add(id);orders.setdefault(identity[0],[]).append(identity[1])
assert all(len(v)==1 for v in partitions.values())
assert all(v==sorted(v) and len(v)==len(set(v)) for v in orders.values()),orders
final=[]
for line in (run/'final.tsv').read_text().splitlines():
 id,call,seq,state,attempts,error,created=line.split('\t');assert id in admissions;final.append(dict(event_id=id,call=call,sequence=int(seq),state=state,error=error))
if scenario=='poison':
 assert {(r['call'],r['sequence']) for r in records}=={('call-1',n) for n in range(events)}
 assert any(x['call']=='call-0' and x['sequence']==0 and x['state']=='dead' for x in final)
 assert len(final)==events
elif scenario=='ttl':
 assert any(x['state']=='dead' and x['error']=='expired_ttl' for x in final)
 for call,seqs in orders.items():assert seqs==list(range(len(seqs)))
else:assert len(seen)==len(plan) and not final
assert seen|{x['event_id'] for x in final}==set(admissions)
assert not seen&{x['event_id'] for x in final}
sender=json.loads((run/('sender-resume.json' if restarted else 'sender.json')).read_text())
if scenario=='rebuild':assert sender['rebuilds']>=1
if scenario in ('cut','rebuild','restart','ttl','blackhole'):assert fault_start and fault_end
summary=dict(name=name,scenario=scenario,command=command,calls=calls,events_per_call=events,source_events=len(plan),records=len(records),unique=len(seen),duplicates=len(duplicates),dead=sum(x['state']=='dead' for x in final),blocked_pending=sum(x['state']=='pending' for x in final),strict_first_order=True,raw_nondecreasing=all(v==sorted(v) for v in allorders.values()),source_sequence_headers_match=True,partitions={k:sorted(v) for k,v in partitions.items()},elapsed_s=time.monotonic()-started,peak_rss_kib=peak_rss,peak_outbox_disk_bytes=peak_disk,admission_p50_us=statistics.median(latency),admission_p95_us=sorted(latency)[int(len(latency)*.95)],admission_max_us=max(latency),processes=processes,sender=sender,binary_sha256=hashlib.sha256(driver.read_bytes()).hexdigest(),pass_expected_contract=True)
(run/'result.json').write_text(json.dumps(summary,indent=2));(run/'duplicates.json').write_text(json.dumps(duplicates,indent=2));print(json.dumps(summary),flush=True)
