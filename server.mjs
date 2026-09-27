import http from 'node:http';
import net from 'node:net';
import tls from 'node:tls';
import crypto from 'node:crypto';
import nodemailer from 'nodemailer';
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
  listen:cleanEnv('OUTPUT_LISTEN_URL'),
  smtpHost:cleanEnv('SMTP_HOST'),
  smtpPort:Number(cleanEnv('SMTP_PORT','587')),
  smtpSecure:cleanEnv('SMTP_SECURE','false').toLowerCase()==='true',
  smtpUser:cleanEnv('SMTP_USER'),
  smtpPass:cleanEnv('SMTP_PASSWORD'),
  emailFrom:cleanEnv('EMAIL_FROM'),
  publicUrl:cleanEnv('APP_PUBLIC_URL','https://signalflow-live-web-production.up.railway.app')
};
let active=null,lastError='';
const liveListeners=new Set();
const RECONNECT_DELAY_MS=2000;
const dataDir=fs.existsSync('/data')?'/data':path.join(__dirname,'.data');
const broadcastersFile=path.join(dataDir,'broadcasters.json');
const auditFile=path.join(dataDir,'broadcast-history.json');
try{fs.mkdirSync(dataDir,{recursive:true});}catch{}
function loadJson(file,fallback){try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch{return fallback;}}
function saveJson(file,value){fs.writeFileSync(file+'.tmp',JSON.stringify(value,null,2));fs.renameSync(file+'.tmp',file);}
function keyHash(value){return crypto.createHash('sha256').update(String(value)).digest('hex');}
function encryptionKey(){return crypto.createHash('sha256').update('signalflow:'+cfg.token).digest();}
function encryptKey(value){
 const iv=crypto.randomBytes(12),cipher=crypto.createCipheriv('aes-256-gcm',encryptionKey(),iv);
 const enc=Buffer.concat([cipher.update(String(value),'utf8'),cipher.final()]);
 return [iv.toString('base64url'),cipher.getAuthTag().toString('base64url'),enc.toString('base64url')].join('.');
}
function decryptKey(value){
 try{
  const [iv,tag,enc]=String(value||'').split('.');
  const decipher=crypto.createDecipheriv('aes-256-gcm',encryptionKey(),Buffer.from(iv,'base64url'));
  decipher.setAuthTag(Buffer.from(tag,'base64url'));
  return Buffer.concat([decipher.update(Buffer.from(enc,'base64url')),decipher.final()]).toString('utf8');
 }catch{return '';}
}
function broadcasters(){return loadJson(broadcastersFile,[]);}
function writeBroadcasters(list){saveJson(broadcastersFile,list);}
function auditHistory(){return loadJson(auditFile,[]);}
function addAudit(entry){const list=auditHistory();list.unshift(entry);saveJson(auditFile,list.slice(0,1000));}
function makeBroadcasterKey(){return 'SFL-'+crypto.randomBytes(18).toString('base64url');}
function authenticateToken(token){
 if(cfg.token&&token===cfg.token)return {id:'admin',name:'Administrator',email:'',role:'admin',status:'active'};
 const h=keyHash(token||'');
 const user=broadcasters().find(x=>x.status==='active'&&x.keyHash===h);
 return user?{id:user.id,name:user.name,email:user.email,role:'broadcaster',status:user.status}:null;
}
function bearer(req){const h=String(req.headers.authorization||'');return h.startsWith('Bearer ')?h.slice(7):'';}
function readBody(req){return new Promise((resolve,reject)=>{let b='';req.on('data',d=>{b+=d;if(b.length>100000)reject(new Error('Request too large'));});req.on('end',()=>{try{resolve(b?JSON.parse(b):{});}catch{reject(new Error('Invalid JSON'));}});req.on('error',reject);});}
function emailConfigured(){return Boolean(cfg.smtpHost&&cfg.smtpUser&&cfg.smtpPass&&cfg.emailFrom);}
let mailer=null;
async function sendInvite(user,key){
 if(!emailConfigured())return {sent:false,reason:'Email delivery is not configured yet'};
 if(!mailer)mailer=nodemailer.createTransport({host:cfg.smtpHost,port:cfg.smtpPort,secure:cfg.smtpSecure,auth:{user:cfg.smtpUser,pass:cfg.smtpPass}});
 const subject='Your SignalFlow Live broadcaster access';
 const text='Hello '+user.name+',\n\nYou now have access to SignalFlow Live.\n\nOpen: '+cfg.publicUrl+'\nBroadcaster key: '+key+'\n\nHow to broadcast:\n1. Open SignalFlow Live and enter your broadcaster key.\n2. Select Microphone and choose/check your microphone.\n3. Press Go Live.\n4. Wait until the status changes to ON AIR before starting.\n5. When finished, press End Broadcast.\n\nYour key is personal to you, so please do not share it.\n';
 await mailer.sendMail({from:cfg.emailFrom,to:user.email,subject,text});
 return {sent:true};
}
function publicBroadcaster(x){return {id:x.id,name:x.name,email:x.email,status:x.status,createdAt:x.createdAt,lastBroadcastAt:x.lastBroadcastAt||null};}

function json(res,code,obj){res.writeHead(code,{'content-type':'application/json','cache-control':'no-store'});res.end(JSON.stringify(obj));}
function typeFor(f){return f.endsWith('.html')?'text/html; charset=utf-8':f.endsWith('.js')?'text/javascript; charset=utf-8':f.endsWith('.css')?'text/css; charset=utf-8':f.endsWith('.webmanifest')?'application/manifest+json':'application/octet-stream';}
function serve(req,res){
 const u=new URL(req.url,'http://localhost');
 if(u.pathname==='/listen'){
   if(!active||!active.ready){
     res.writeHead(503,{'content-type':'text/plain','cache-control':'no-store, no-cache, must-revalidate, max-age=0','pragma':'no-cache','expires':'0','access-control-allow-origin':'*'});
     return res.end('SignalFlow Live is off air');
   }
   res.writeHead(200,{
     'content-type':cfg.format==='aac'?'audio/aac':'audio/mpeg',
     'cache-control':'no-store, no-cache, must-revalidate, max-age=0',
     'pragma':'no-cache',
     'expires':'0',
     'access-control-allow-origin':'*',
     'x-accel-buffering':'no',
     'connection':'keep-alive'
   });
   if(res.flushHeaders)res.flushHeaders();
   liveListeners.add(res);
   req.on('close',()=>liveListeners.delete(res));
   return;
 }
 if(u.pathname==='/api/status'){
   const proto=(req.headers['x-forwarded-proto']||'https').split(',')[0].trim();
   const host=req.headers.host||'localhost';
   const directListenUrl=proto+'://'+host+'/listen';
   return json(res,200,{
     ok:true,configured:Boolean(cfg.host&&cfg.pass),active:Boolean(active&&active.ready),stationName:cfg.station,
     connectionState:active?active.connectionState:'idle',reconnects:active?.reconnects||0,
     outputFormat:cfg.format,bitrate:cfg.bitrate,listenUrl:directListenUrl,directListenUrl:directListenUrl,mount:cfg.mount,lastError
   });
 }
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
function connectShoutcast(session){
 if(!session||session.stopping||session.ws.readyState!==1)return;
 session.connectionState=session.everReady?'reconnecting':'connecting';
 session.ready=false;
 const connectOpts={host:cfg.host,port:cfg.icePort};
 const sock=cfg.tls?tls.connect({...connectOpts,servername:cfg.host}):net.connect(connectOpts);
 session.sock=sock;
 let accepted=false,reply='',finished=false;
 const scheduleReconnect=(reason)=>{
   if(finished)return;
   finished=true;
   try{sock.destroy();}catch{}
   if(session.stopping||active!==session)return;
   session.ready=false;
   session.connectionState='reconnecting';
   session.reconnects=(session.reconnects||0)+1;
   if(reason){lastError=reason;console.error('Shoutcast source connection lost:',reason);}
   clearTimeout(session.reconnectTimer);
   session.reconnectTimer=setTimeout(()=>connectShoutcast(session),RECONNECT_DELAY_MS);
 };
 sock.setTimeout(10000,()=>scheduleReconnect('Shoutcast source handshake timed out'));
 sock.once('error',e=>scheduleReconnect(e?.message||String(e)));
 sock.once('close',()=>{if(accepted)scheduleReconnect('Shoutcast source connection closed');});
 sock.once('connect',()=>sock.write(cfg.pass+'\n'));
 sock.on('data',chunk=>{
   if(accepted||finished)return;
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
     accepted=true;
     sock.setTimeout(0);
     sock.write(headers);
     session.ready=true;
     session.everReady=true;
     session.connectionState='live';
     lastError='';
     console.log('Shoutcast v1 source accepted');
   }else if(/invalid password|bad password|denied|error/i.test(reply)){
     scheduleReconnect('Shoutcast rejected source: '+reply.trim().slice(0,160));
   }
 });
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
 const session={ws,proc,sock:null,ready:!isShoutcast,everReady:!isShoutcast,connectionState:isShoutcast?'connecting':'live',reconnects:0,reconnectTimer:null,stopping:false,since:new Date().toISOString()};
 active=session; lastError='';
 proc.stderr.setEncoding('utf8');
 proc.stderr.on('data',d=>{const t=String(d).trim();if(t){lastError=t.slice(-800);console.error(t.replaceAll(cfg.pass,'[REDACTED]'));}});
 if(isShoutcast){
   proc.stdout.on('data',chunk=>{
     if(session.sock&&session.ready&&session.sock.writable){
       try{session.sock.write(chunk);}catch{}
     }
     for(const listener of Array.from(liveListeners)){
       try{
         if(!listener.writableEnded)listener.write(chunk);
         else liveListeners.delete(listener);
       }catch{liveListeners.delete(listener);}
     }
   });
   connectShoutcast(session);
 }
 proc.on('exit',(code)=>{
   console.log('ffmpeg exit',code);
   if(active===session){
     session.stopping=true;
     clearTimeout(session.reconnectTimer);
     try{session.sock?.destroy();}catch{}
     active=null;
   }
 });
 ws.on('message',d=>{if(proc.stdin.writable)proc.stdin.write(Buffer.isBuffer(d)?d:Buffer.from(d));});
 const stop=()=>{
   if(active!==session)return;
   session.stopping=true;
   clearTimeout(session.reconnectTimer);
   try{proc.stdin.end();}catch{}
   try{session.sock?.end();}catch{}
   setTimeout(()=>{if(!proc.killed)proc.kill('SIGTERM')},800).unref();
   active=null;
 };
 ws.on('close',stop);
 ws.on('error',e=>{lastError=e.message||String(e);stop();});
});
server.listen(cfg.port,'0.0.0.0',()=>console.log(`SignalFlow Live listening on :${cfg.port}`));
