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
const audioFile=path.join(dataDir,'audio-library.json');
const audioDir=path.join(dataDir,'audio');
try{fs.mkdirSync(dataDir,{recursive:true});fs.mkdirSync(audioDir,{recursive:true});}catch{}
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
function audioLibrary(){return loadJson(audioFile,[]);}
function writeAudioLibrary(list){saveJson(audioFile,list);}
function safeAudioName(name){return String(name||'audio').replace(/[^a-zA-Z0-9._ -]/g,'_').slice(0,120);}
function readBinary(req,maxBytes=50*1024*1024){return new Promise((resolve,reject)=>{const chunks=[];let size=0;req.on('data',d=>{size+=d.length;if(size>maxBytes){reject(new Error('Audio file is too large'));req.destroy();return;}chunks.push(d);});req.on('end',()=>resolve(Buffer.concat(chunks)));req.on('error',reject);});}
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
async function handleApi(req,res,u){
 const token=bearer(req),who=authenticateToken(token);
 if(u.pathname==='/api/me'){
   if(!who)return json(res,401,{error:'Unauthorized'});
   return json(res,200,{id:who.id,name:who.name,email:who.email,role:who.role});
 }
 if(u.pathname==='/api/audio'&&req.method==='GET'){
   if(!who)return json(res,401,{error:'Unauthorized'});
   const list=audioLibrary().map(x=>({id:x.id,name:x.name,mime:x.mime,size:x.size,createdAt:x.createdAt,ownerName:x.ownerName,hotkey:!!x.hotkey,hotkeyOrder:x.hotkeyOrder||null,url:'/media/'+x.id+'?token='+encodeURIComponent(token)}));
   return json(res,200,{audio:list,hotkeys:list.filter(x=>x.hotkey).sort((a,b)=>(a.hotkeyOrder||99)-(b.hotkeyOrder||99))});
 }
 if(u.pathname==='/api/audio'&&req.method==='POST'){
   if(!who)return json(res,401,{error:'Unauthorized'});
   try{
     const mime=String(req.headers['content-type']||'').toLowerCase();
     if(!/^audio\//.test(mime)&&mime!=='application/octet-stream')return json(res,415,{error:'Please upload an audio file'});
     const original=safeAudioName(decodeURIComponent(String(req.headers['x-file-name']||'audio')));
     const data=await readBinary(req);
     if(!data.length)return json(res,400,{error:'The audio file was empty'});
     const id=crypto.randomUUID(),ext=path.extname(original).slice(0,10)||'.audio';
     fs.writeFileSync(path.join(audioDir,id+ext),data);
     const item={id,name:path.basename(original,path.extname(original))||'Audio',originalName:original,ext,mime:mime==='application/octet-stream'?'audio/mpeg':mime,size:data.length,createdAt:new Date().toISOString(),ownerId:who.id,ownerName:who.name,hotkey:false,hotkeyOrder:null};
     const list=audioLibrary();list.push(item);writeAudioLibrary(list);
     return json(res,201,{audio:{...item,url:'/media/'+id+'?token='+encodeURIComponent(token)}});
   }catch(e){return json(res,400,{error:e.message||String(e)});}
 }
 const audioMatch=u.pathname.match(/^\/api\/audio\/([^/]+)\/(hotkey|delete)$/);
 if(audioMatch&&req.method==='POST'){
   if(!who)return json(res,401,{error:'Unauthorized'});
   const [,id,action]=audioMatch,list=audioLibrary(),idx=list.findIndex(x=>x.id===id);
   if(idx<0)return json(res,404,{error:'Audio not found'});
   if(action==='hotkey'){
     if(who.role!=='admin')return json(res,403,{error:'Admin access required'});
     let body={};try{body=await readBody(req);}catch{}
     list[idx].hotkey=!!body.enabled;
     list[idx].hotkeyOrder=list[idx].hotkey?Math.max(1,Math.min(12,Number(body.order)||1)):null;
     writeAudioLibrary(list);return json(res,200,{ok:true});
   }
   if(action==='delete'){
     if(who.role!=='admin'&&list[idx].ownerId!==who.id)return json(res,403,{error:'Not allowed'});
     try{fs.unlinkSync(path.join(audioDir,list[idx].id+list[idx].ext));}catch{}
     list.splice(idx,1);writeAudioLibrary(list);return json(res,200,{ok:true});
   }
 }
 if(!who||who.role!=='admin')return json(res,403,{error:'Admin access required'});
 if(u.pathname==='/api/broadcasters'&&req.method==='GET'){
   return json(res,200,{broadcasters:broadcasters().map(publicBroadcaster),emailConfigured:emailConfigured()});
 }
 if(u.pathname==='/api/broadcast-history'&&req.method==='GET'){
   return json(res,200,{history:auditHistory().slice(0,200)});
 }
 if(u.pathname==='/api/broadcasters'&&req.method==='POST'){
   try{
     const body=await readBody(req),name=String(body.name||'').trim(),email=String(body.email||'').trim().toLowerCase();
     if(!name||!email||!email.includes('@'))return json(res,400,{error:'Name and a valid email address are required'});
     const list=broadcasters();
     if(list.some(x=>x.email.toLowerCase()===email&&x.status==='active'))return json(res,409,{error:'An active broadcaster already uses that email address'});
     const rawKey=makeBroadcasterKey(),now=new Date().toISOString();
     const user={id:crypto.randomUUID(),name,email,keyHash:keyHash(rawKey),encryptedKey:encryptKey(rawKey),status:'active',createdAt:now,lastBroadcastAt:null};
     list.push(user);writeBroadcasters(list);
     let invite={sent:false,reason:'Invite not requested'};
     if(body.sendEmail!==false){try{invite=await sendInvite(user,rawKey);}catch(e){invite={sent:false,reason:e.message||String(e)};}}
     return json(res,201,{broadcaster:publicBroadcaster(user),key:rawKey,invite});
   }catch(e){return json(res,400,{error:e.message||String(e)});}
 }
 const match=u.pathname.match(/^\/api\/broadcasters\/([^/]+)\/(revoke|activate|regenerate|resend)$/);
 if(match&&req.method==='POST'){
   const [,id,action]=match,list=broadcasters(),idx=list.findIndex(x=>x.id===id);
   if(idx<0)return json(res,404,{error:'Broadcaster not found'});
   const user=list[idx];
   if(action==='revoke'){user.status='revoked';writeBroadcasters(list);return json(res,200,{broadcaster:publicBroadcaster(user)});}
   if(action==='activate'){user.status='active';writeBroadcasters(list);return json(res,200,{broadcaster:publicBroadcaster(user)});}
   if(action==='regenerate'){
     const rawKey=makeBroadcasterKey();user.keyHash=keyHash(rawKey);user.encryptedKey=encryptKey(rawKey);user.status='active';writeBroadcasters(list);
     let invite={sent:false,reason:'Invite not requested'};
     let body={};try{body=await readBody(req);}catch{}
     if(body.sendEmail!==false){try{invite=await sendInvite(user,rawKey);}catch(e){invite={sent:false,reason:e.message||String(e)};}}
     return json(res,200,{broadcaster:publicBroadcaster(user),key:rawKey,invite});
   }
   if(action==='resend'){
     const rawKey=decryptKey(user.encryptedKey);
     if(!rawKey)return json(res,409,{error:'This key cannot be recovered. Regenerate the key instead.'});
     try{const invite=await sendInvite(user,rawKey);return json(res,200,{broadcaster:publicBroadcaster(user),invite});}
     catch(e){return json(res,502,{error:e.message||String(e)});}
   }
 }
 return json(res,404,{error:'Not found'});
}
function finaliseAudit(session){
 if(!session||session.auditSaved)return;
 session.auditSaved=true;
 const end=new Date(),start=new Date(session.since);
 const entry={id:crypto.randomUUID(),broadcasterId:session.broadcaster.id,broadcasterName:session.broadcaster.name,broadcasterEmail:session.broadcaster.email||'',startedAt:session.since,endedAt:end.toISOString(),durationSeconds:Math.max(0,Math.round((end-start)/1000)),reconnects:session.reconnects||0};
 try{addAudit(entry);}catch(e){console.error('Could not save broadcast history:',e.message||String(e));}
 if(session.broadcaster.role==='broadcaster'){
   try{const list=broadcasters(),idx=list.findIndex(x=>x.id===session.broadcaster.id);if(idx>=0){list[idx].lastBroadcastAt=end.toISOString();writeBroadcasters(list);}}catch(e){console.error('Could not update broadcaster history:',e.message||String(e));}
 }
}
function serve(req,res){
 const u=new URL(req.url,'http://localhost');
 if(u.pathname==='/api/me'||u.pathname==='/api/audio'||u.pathname==='/api/broadcasters'||u.pathname==='/api/broadcast-history'||u.pathname.startsWith('/api/audio/')||u.pathname.startsWith('/api/broadcasters/')) return void handleApi(req,res,u);
 if(u.pathname.startsWith('/media/')){
   const token=u.searchParams.get('token')||'',who=authenticateToken(token);
   if(!who){res.writeHead(401);return res.end('Unauthorized');}
   const id=u.pathname.slice('/media/'.length),item=audioLibrary().find(x=>x.id===id);
   if(!item){res.writeHead(404);return res.end('Not found');}
   const file=path.join(audioDir,item.id+item.ext);
   try{
     const stat=fs.statSync(file);
     res.writeHead(200,{'content-type':item.mime||'audio/mpeg','content-length':stat.size,'accept-ranges':'bytes','cache-control':'private, max-age=3600'});
     return fs.createReadStream(file).pipe(res);
   }catch{res.writeHead(404);return res.end('Not found');}
 }
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
     connectionState:active?active.connectionState:'idle',reconnects:active?.reconnects||0,currentBroadcaster:active?.broadcaster?.name||null,
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
 const broadcaster=authenticateToken(token);
 if(!broadcaster){socket.write('HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n');return socket.destroy();}
 if(active){socket.write('HTTP/1.1 409 Conflict\r\nConnection: close\r\n\r\n');return socket.destroy();}
 if(!cfg.host||!cfg.pass){socket.write('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');return socket.destroy();}
 wss.handleUpgrade(req,socket,head,ws=>{ws.broadcaster=broadcaster;wss.emit('connection',ws);});
});
wss.on('connection',ws=>{
 const isShoutcast=cfg.serverType==='shoutcast';
 const proc=spawn('ffmpeg',isShoutcast?encoderArgs('pipe:1'):icecastArgs(),{stdio:['pipe',isShoutcast?'pipe':'ignore','pipe']});
 const session={ws,proc,sock:null,ready:!isShoutcast,everReady:!isShoutcast,connectionState:isShoutcast?'connecting':'live',reconnects:0,reconnectTimer:null,stopping:false,since:new Date().toISOString(),broadcaster:ws.broadcaster||{id:'unknown',name:'Unknown',email:'',role:'broadcaster'}};
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
     finaliseAudit(session);
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
   finaliseAudit(session);
   active=null;
 };
 ws.on('close',stop);
 ws.on('error',e=>{lastError=e.message||String(e);stop();});
});
server.listen(cfg.port,'0.0.0.0',()=>console.log(`SignalFlow Live listening on :${cfg.port}`));
