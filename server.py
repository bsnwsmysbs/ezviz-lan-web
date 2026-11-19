from __future__ import annotations
import json, os, re, threading, time, subprocess
from queue import Queue
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / 'devices.local.json'
DEVICES = json.loads(CONFIG.read_text(encoding='utf-8-sig')) if CONFIG.exists() else {}
if not isinstance(DEVICES, dict) or any(not re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', key) for key in DEVICES):
    raise ValueError('Invalid device configuration name')
OUT = Path(os.environ.get('EZVIZ_H5_OUTPUT', ROOT / 'recordings'))
state={"status":"未开始","done":0,"total":0,"bytes":0,"speed":0.0,"failed":0,"processed":0,"newLogs":[],"lock":threading.Lock(),"running":False}

JOBS = []
PENDING = Queue()
ACTIVE = None


def run_queue():
    global ACTIVE
    while True:
        job, cfg = PENDING.get()
        with state['lock']:
            ACTIVE = job['id']
            state.update(status='读取目录中', done=0, failed=0, processed=0, total=0,
                         bytes=0, speed=0.0, newLogs=[], running=True)
        try:
            worker(cfg)
        finally:
            with state['lock']:
                job.update({key: state[key] for key in ('status','done','failed','processed','total')})
                ACTIVE = None
            PENDING.task_done()


def addlog(s):
    with state['lock']:
        state['newLogs']=(state['newLogs']+[s])[-160:]

def worker(cfg):
    try:
        from pyezvizapi import HcNetSdkLanEndpoint, download_lan_sd_recording
        from pyezvizapi.streaming.local.recordings import query_lan_recordings
        from pyezvizapi.streaming.local.hcnetsdk.api import ezviz_lan_local_user_password
        from zoneinfo import ZoneInfo
        import imageio_ffmpeg
        clock=ZoneInfo("Asia/Shanghai"); fmt="%Y%m%dT%H%M%SZ"
        start=datetime.fromisoformat(cfg['from']).replace(tzinfo=clock)
        end=datetime.fromisoformat(cfg['to']).replace(tzinfo=clock)
        if len(cfg['to'])==10:
            end+=timedelta(days=1)
        selections=[]
        for key in cfg["cameras"]:
            d=DEVICES[key]; code=cfg.get(key+"Code","").strip()
            if not code: raise RuntimeError(f"{d['name']} 缺少验证码")
            ep=HcNetSdkLanEndpoint(serial=d["serial"],host=d["host"],command_port=8000); pwd=ezviz_lan_local_user_password(code)
            addlog(f"正在读取 {d['name']} 的录像目录")
            day=start
            while day<end:
                next_day=min(end, day.replace(hour=0)+timedelta(days=1))
                first,last=(v.astimezone(timezone.utc).strftime(fmt) for v in (day,next_day))
                try:
                    cat=query_lan_recordings(ep,pwd,first,last,channel=1,timezone=clock.key,username='EZ_LOCAL_USER',transfer_timeout=30,timeout=12)
                except Exception as error:
                    with state['lock']:
                        state['failed']+=1
                    addlog(f"{d['name']} {day:%Y-%m-%d} 查询失败（{type(error).__name__}）")
                    if 'login failed' in str(error).lower():
                        break
                else:
                    for item in cat.recordings:
                        begin,finish=(datetime.strptime(v,fmt).replace(tzinfo=timezone.utc) for v in (max(first,item.begin_time),min(last,item.end_time)))
                        while begin<finish:
                            stop=min(begin+timedelta(seconds=60),finish)
                            selections.append((key,d,ep,pwd,begin,stop))
                            begin=stop
                day=next_day
        selections=list({(key,a,b):(key,d,ep,pwd,a,b) for key,d,ep,pwd,a,b in selections}.values())
        with state['lock']:
            state.update(total=len(selections),status='下载中')
        ffmpeg=imageio_ffmpeg.get_ffmpeg_exe()
        for index,(key,d,ep,pwd,begin,finish) in enumerate(selections,1):
            local=begin.astimezone(clock)
            folder=OUT/key/local.date().isoformat()
            folder.mkdir(parents=True,exist_ok=True)
            stem=f'{local:%H%M%S}-{finish.astimezone(clock):%H%M%S}'
            video,raw,tmp=(folder/(stem+ext) for ext in ('.mp4','.part.ps','.part.mp4'))
            label=f"{index}/{len(selections)} {d['name']} {local:%m-%d %H:%M:%S}"
            try:
                if video.is_file() and video.stat().st_size:
                    with state['lock']:
                        state['done']+=1
                    continue
                addlog(label+' 正在下载')
                tick=time.monotonic()
                receipt=download_lan_sd_recording(ep,pwd,raw,begin.strftime(fmt),finish.strftime(fmt),channel=1,timezone=clock.key,username='EZ_LOCAL_USER',timeout=15,transfer_timeout=max(45,(finish-begin).total_seconds()*2),max_bytes=256*1024**2)
                speed=receipt['bytes']/1e6/max(0.01,time.monotonic()-tick)
                subprocess.run([ffmpeg,'-nostdin','-v','error','-y','-i',str(raw),'-map','0:v:0','-map','0:a:0?','-c','copy',str(tmp)],check=True,capture_output=True,timeout=45)
                tmp.replace(video)
                with state['lock']:
                    state['done']+=1
                    state['bytes']+=video.stat().st_size
                    state['speed']=speed
                addlog(f'{label} 已保存 · {speed:.2f} MB/s')
            except Exception as error:
                with state['lock']:
                    state['failed']+=1
                addlog(f'{label} 未保存（{type(error).__name__}）')
            finally:
                raw.unlink(missing_ok=True)
                tmp.unlink(missing_ok=True)
                with state['lock']:
                    state['processed']=index
        with state['lock']:
            state.update(status='结束，有失败' if state['failed'] else ('完成' if selections else '没有录像'),running=False)
    except Exception as error:
        addlog(f'任务中断（{type(error).__name__}）')
        with state['lock']:
            state.update(status='失败',running=False)


class Handler(BaseHTTPRequestHandler):
    def log_message(self,*args):
        pass

    def send(self,status,kind,content):
        self.send_response(status)
        self.send_header('Content-Type',kind)
        self.send_header('Cache-Control','no-store')
        self.send_header('Content-Length',str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def reply(self,http_status,**body):
        self.send(http_status,'application/json; charset=utf-8',json.dumps(body,ensure_ascii=False).encode())

    def do_GET(self):
        if self.path=='/':
            return self.send(200,'text/html; charset=utf-8',(ROOT/'index.html').read_bytes())
        if self.path!='/api/status':
            return self.reply(404,error='页面不存在')
        with state['lock']:
            snapshot={key:value for key,value in state.items() if key!='lock'}
            jobs=[job.copy() for job in JOBS]
            if ACTIVE is not None:
                jobs[ACTIVE-1].update({key:state[key] for key in ('status','total','processed','done','failed')})
        self.reply(200,**snapshot,output=str(OUT),jobs=jobs,devices={key: device["name"] for key, device in DEVICES.items()})

    def do_POST(self):
        if self.path!='/api/start':
            return self.reply(404,error='页面不存在')
        if self.headers.get('Origin')!=f'http://127.0.0.1:{self.server.server_port}' or self.headers.get_content_type()!='application/json':
            return self.reply(403,error='请从本机页面提交')
        try:
            length=int(self.headers.get('Content-Length',0))
            if not 0<length<=4096:
                raise ValueError
            cfg=json.loads(self.rfile.read(length))
            first,last=(datetime.strptime(cfg[k], '%Y-%m-%d' if len(cfg[k])==10 else '%Y-%m-%dT%H:00') for k in ('from','to'))
            if len(cfg['to'])==10:
                last+=timedelta(days=1)
            if first>=last or last.year==9999:
                raise ValueError
            keys=cfg['cameras']
            if not isinstance(keys,list) or not 1<=len(keys)<=len(DEVICES) or any(k not in DEVICES for k in keys) or len(set(keys))!=len(keys):
                raise ValueError
            for key in keys:
                code=cfg.get(key+'Code','').strip()
                if len(code)!=6 or not code.isascii() or not code.isalnum():
                    return self.reply(400,error=f"请输入 {DEVICES[key]['name']} 的 6 位验证码")
                cfg[key+'Code']=code
        except (TypeError,ValueError,AttributeError,KeyError,OverflowError):
            return self.reply(400,error='请选择有效的整点时间，结束须晚于开始')
        cfg.update({'from':first.isoformat(timespec='minutes'),'to':last.isoformat(timespec='minutes')})
        with state['lock']:
            job=dict(id=len(JOBS)+1,cameras=[DEVICES[k]['name'] for k in keys],
                     start=first.strftime('%Y-%m-%d %H:%M'),end=last.strftime('%Y-%m-%d %H:%M'),
                     status='排队中',done=0,failed=0,processed=0,total=0)
            JOBS.append(job)
            PENDING.put((job,cfg))
        self.reply(200,ok=True,id=job['id'])


if __name__=='__main__':
    server=ThreadingHTTPServer(('127.0.0.1',8765),Handler)
    threading.Thread(target=run_queue,daemon=True).start()
    server.serve_forever()



