import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';

const __dirname=path.dirname(fileURLToPath(import.meta.url));
const publicDir=path.join(__dirname,'public');
const cfg={
  port:Number(process.env.PORT||8080),
  token:process.env.BROADCAST_TOKEN||'',
  host:process.env.ICECAST_HOST||'',
  icePort:Number(process.env.ICECAST_PORT||8000),
  tls:String(process.env.ICECAST_TLS||'false').toLowerCase()==='true',
  mount:(process.env.ICECAST_MOUNT||'/mobile-live.mp3').replace(/^([^/])/,'/$1'),
  user:process.env.ICECAST_SOURCE_USER||'source',
  pass:process.env.ICECAST_SOURCE_PASSWORD||'',
  format:(process.env.OUTPUT_FORMAT||'mp3').toLowerCase(),
  bitrate:process.env.OUTPUT_BITRATE||'128k',
  rate:process.env.OUTPUT_SAMPLE_RATE||'44100',
  channels:process.env.OUTPUT_CHANNELS||'2',
  station:process.env.STATION_NAME||'SignalFlow Live',
  listen:process.env.OUTPUT_LISTEN_URL||''
};
let active=null,lastError='';

function json(res,code,obj){res.writeHead(code,{'content-type':'application/json','cache-control':'no-store'});res.end(JSON.stringify(obj));}
function typeFor(f){return f.endsWith('.html')?'text/html; charset=utf-8':f.endsWith('.js')?'text/javascript; charset=utf-8':f.endsWith('.css')?'text/css; charset=utf-8':f.endsWith('.webmanifest')?'application/manifest+json':'application/octet-stream';}
function serve(req,res){
 const u=new URL(req.url,'http://localhost');
 if(u.pathname==='/api/status') return json(res,200,{
   ok:true,configured:Boolean(cfg.host&&cfg.pass),active:Boolean(active),stationName:cfg.station,
   outputFormat:cfg.format,bitrate:cfg.bitrate,listenUrl:cfg.listen,mount:cfg.mount,lastError
 });
 let p=u.pathname==='/'?'/index.html':u.pathname;
 p=path.normalize(p).replace(/^(..[/\\])+/, '');
 const file=path.join(publicDir,p);
 if(!file.startsWith(publicDir)) return json(res,403,{error:'forbidden'});
 fs.readFile(file,(err,data)=>{
   if(err){res.writeHead(404);res.end('Not found');return;}
   res.writeHead(200,{'content-type':typeFor(file),'cache-control':p==='/index.html'?'no-cache':'public, max-age=300'});
   res.end(data);
 });
}
function target(){
 const scheme=cfg.tls?'icecasts':'icecast';
 return `${scheme}://${encodeURIComponent(cfg.user)}:${encodeURIComponent(cfg.pass)}@${cfg.host}:${cfg.icePort}${cfg.mount}`;
}
function args(){
 const common=['-hide_banner','-loglevel','warning','-i','pipe:0','-vn','-ar',cfg.rate,'-ac',cfg.channels];
 if(cfg.format==='aac') return [...common,'-c:a','aac','-b:a',cfg.bitrate,'-content_type','audio/aac','-f','adts',target()];
 return [...common,'-c:a','libmp3lame','-b:a',cfg.bitrate,'-content_type','audio/mpeg','-f','mp3',target()];
}
const server=http.createServer(serve);
const wss=new WebSocketServer({noServer:true});
server.on('upgrade',(req,socket,head)=>{
 const u=new URL(req.url,`http://${req.headers.host||'localhost'}`);
 if(u.pathname!=='/live') return socket.destroy();
 const token=u.searchParams.get('token')||'';
 if(!cfg.token||token!==cfg.token){socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');return socket.destroy();}
 if(active){socket.write('HTTP/1.1 409 Conflict\r\nConnection: close\r\n\r\n');return socket.destroy();}
 if(!cfg.host||!cfg.pass){socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');return socket.destroy();}
 wss.handleUpgrade(req,socket,head,ws=>wss.emit('connection',ws));
});
wss.on('connection',ws=>{
 const proc=spawn('ffmpeg',args(),{stdio:['pipe','ignore','pipe']});
 active={ws,proc,since:new Date().toISOString()}; lastError='';
 proc.stderr.setEncoding('utf8');
 proc.stderr.on('data',d=>{const t=String(d).trim();if(t){lastError=t.slice(-800);console.error(t.replaceAll(cfg.pass,'[REDACTED]'));}});
 proc.on('exit',(code)=>{console.log('ffmpeg exit',code);if(active?.proc===proc)active=null;});
 ws.on('message',d=>{if(proc.stdin.writable)proc.stdin.write(Buffer.isBuffer(d)?d:Buffer.from(d));});
 const stop=()=>{if(active?.ws!==ws)return;try{proc.stdin.end();}catch{}setTimeout(()=>{if(!proc.killed)proc.kill('SIGTERM')},800).unref();active=null;};
 ws.on('close',stop);ws.on('error',e=>{lastError=e.message||String(e);stop();});
});
server.listen(cfg.port,'0.0.0.0',()=>console.log(`SignalFlow Live listening on :${cfg.port}`));
