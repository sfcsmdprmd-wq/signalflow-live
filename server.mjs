import http from 'node:http';
import net from 'node:net';
import tls from 'node:tls';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';

const __dirname=path.dirname(fileURLToPath(import.meta.url));
const publicDir=path.join(__dirname,'public');
function cleanEnv(name,fallback=''){
  const raw=process.env[name];
  if(raw==null)return fallback;
  const v=String(raw).trim();
  if((v.startsWith('"')&&v.endsWith('"'))||(v.startsWith("'")&&v.endsWith("'"))) return v.slice(1,-1).trim();
  return v;
}
const cfg={
  serverType:cleanEnv('STREAM_SERVER_TYPE','icecast').toLowerCase(),
  port:Number(cleanEnv('PORT','8080')),
  token:cleanEnv('BROADCAST_TOKEN'),
  host:cleanEnv('ICECAST_HOST'),
  icePort:Number(cleanEnv('ICECAST_PORT','8000')),
  tls:cleanEnv('ICECAST_TLS','false').toLowerCase()==='true',
  mount:cleanEnv('ICECAST_MOUNT','/mobile-live.mp3').replace(/^([^/])/,'/$1'),
  user:cleanEnv('ICECAST_SOURCE_USER','source'),
  pass:cleanEnv('ICECAST_SOURCE_PASSWORD'),
  format:cleanEnv('OUTPUT_FORMAT','mp3').toLowerCase(),
  bitrate:cleanEnv('OUTPUT_BITRATE','128k'),
  rate:cleanEnv('OUTPUT_SAMPLE_RATE','44100'),
  channels:cleanEnv('OUTPUT_CHANNELS','2'),
  station:cleanEnv('STATION_NAME','SignalFlow Live'),
  listen:cleanEnv('OUTPUT_LISTEN_URL')
};
let active=null,lastError='';

function json(res,code,obj){res.writeHead(code,{'content-type':'application/json','cache-control':'no-store'});res.end(JSON.stringify(obj));}
function typeFor(f){return f.endsWith('.html')?'text/html; charset=utf-8':f.endsWith('.js')?'text/javascript; charset=utf-8':f.endsWith('.css')?'text/css; charset=utf-8':f.endsWith('.webmanifest')?'application/manifest+json':'application/octet-stream';}
function serve(req,res){
 const u=new URL(req.url,'http://localhost');
 if(u.pathname==='/api/status') return json(res,200,{
   ok:true,configured:Boolean(cfg.host&&cfg.pass),active:Boolean(active&&active.ready),stationName:cfg.station,
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
 return `icecast://${encodeURIComponent(cfg.user)}:${encodeURIComponent(cfg.pass)}@${cfg.host}:${cfg.icePort}${cfg.mount}`;
}
function encoderArgs(outputTarget){
 const common=['-hide_banner','-loglevel','warning','-i','pipe:0','-vn','-ar',cfg.rate,'-ac',cfg.channels];
 if(cfg.format==='aac') return [...common,'-c:a','aac','-b:a',cfg.bitrate,'-f','adts',outputTarget];
 return [...common,'-c:a','libmp3lame','-b:a',cfg.bitrate,'-f','mp3',outputTarget];
}
function icecastArgs(){
 const common=['-hide_banner','-loglevel','warning','-i','pipe:0','-vn','-ar',cfg.rate,'-ac',cfg.channels];
 if(cfg.tls) common.push('-tls','1');
 if(cfg.format==='aac') return [...common,'-c:a','aac','-b:a',cfg.bitrate,'-content_type','audio/aac','-f','adts',target()];
 return [...common,'-c:a','libmp3lame','-b:a',cfg.bitrate,'-content_type','audio/mpeg','-f','mp3',target()];
}
function startShoutcast(proc,onReady,onFail){
 const connectOpts={host:cfg.host,port:cfg.icePort};
 const sock=cfg.tls?tls.connect({...connectOpts,servername:cfg.host}):net.connect(connectOpts);
 let settled=false,reply='';
 const fail=(err)=>{
   if(settled)return;
   settled=true;
   const msg=err?.message||String(err||'Shoutcast connection failed');
   lastError=msg;
   console.error('Shoutcast source error:',msg);
   try{sock.destroy();}catch{}
   try{proc.kill('SIGTERM');}catch{}
   onFail?.(msg);
 };
 sock.setTimeout(10000,()=>fail(new Error('Shoutcast source handshake timed out')));
 sock.once('error',fail);
 sock.once('connect',()=>{
   // SHOUTcast v1 authenticates in two stages:
   // 1) send only the source password
   // 2) wait for OK2, then send icy headers and MP3 bytes
   sock.write(cfg.pass+'\n');
 });
 sock.on('data',chunk=>{
   if(settled)return;
   reply+=chunk.toString('utf8');
   if(/OK2/i.test(reply)){
     const br=String(cfg.bitrate).replace(/[^0-9]/g,'')||'128';
     const contentType=cfg.format==='aac'?'audio/aac':'audio/mpeg';
     const headers=[
       'icy-name:'+cfg.station,
       'icy-genre:',
       'icy-url:',
       'icy-pub:0',
       'icy-br:'+br,
       'content-type:'+contentType,
       ''
     ].join('\n')+'\n';
     settled=true;
     sock.setTimeout(0);
     sock.write(headers);
     proc.stdout.pipe(sock);
     proc.stdout.resume();
     console.log('Shoutcast v1 source accepted');
     onReady?.(sock);
   }else if(/invalid password|bad password|denied|error/i.test(reply)){
     fail(new Error('Shoutcast rejected source: '+reply.trim().slice(0,160)));
   }
 });
 return sock;
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
 const isShoutcast=cfg.serverType==='shoutcast';
 const proc=spawn('ffmpeg',isShoutcast?encoderArgs('pipe:1'):icecastArgs(),{stdio:['pipe',isShoutcast?'pipe':'ignore','pipe']});
 active={ws,proc,sock:null,ready:!isShoutcast,since:new Date().toISOString()}; lastError='';
 if(isShoutcast) proc.stdout.pause();
 proc.stderr.setEncoding('utf8');
 proc.stderr.on('data',d=>{const t=String(d).trim();if(t){lastError=t.slice(-800);console.error(t.replaceAll(cfg.pass,'[REDACTED]'));}});
 proc.on('exit',(code)=>{console.log('ffmpeg exit',code);if(active?.proc===proc){try{active.sock?.destroy();}catch{}active=null;}});
 if(isShoutcast){
   const sock=startShoutcast(proc,
     s=>{if(active?.proc===proc){active.sock=s;active.ready=true;}},
     ()=>{try{ws.close(1011,'Shoutcast source rejected');}catch{}}
   );
   if(active?.proc===proc)active.sock=sock;
 }
 ws.on('message',d=>{if(proc.stdin.writable)proc.stdin.write(Buffer.isBuffer(d)?d:Buffer.from(d));});
 const stop=()=>{if(active?.ws!==ws)return;try{proc.stdin.end();}catch{}try{active.sock?.end();}catch{}setTimeout(()=>{if(!proc.killed)proc.kill('SIGTERM')},800).unref();active=null;};
 ws.on('close',stop);ws.on('error',e=>{lastError=e.message||String(e);stop();});
});
server.listen(cfg.port,'0.0.0.0',()=>console.log(`SignalFlow Live listening on :${cfg.port}`));
