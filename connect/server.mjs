import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const port = Number(process.env.PORT || 8080);
const defaults = [
  {id:'studio1', name:'Studio 1', description:'Main on-air studio'},
  {id:'studio2', name:'Studio 2', description:'Secondary studio'},
  {id:'newsroom', name:'Newsroom', description:'News desk'}
];
function destinations() {
  try {
    const configured = JSON.parse(process.env.CONNECT_DESTINATIONS || 'null');
    if (!Array.isArray(configured)) return defaults;
    return configured.filter(x => x && /^[a-z0-9_-]{1,40}$/i.test(x.id) && typeof x.name === 'string' && x.name.length < 80).slice(0,20).map(x=>({id:x.id,name:x.name,description:String(x.description||'').slice(0,100)}));
  } catch { return defaults; }
}
const html = await readFile(join(here,'public','index.html'));
const server = http.createServer((req,res)=>{
  const url = new URL(req.url || '/', 'http://localhost');
  const send=(status,body,type)=>{res.writeHead(status,{'Content-Type':type,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Content-Security-Policy':"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",'Referrer-Policy':'no-referrer'});res.end(body);};
  if(req.method!=='GET') return send(405,'Method not allowed','text/plain');
  if(url.pathname==='/health') return send(200,'OK','text/plain');
  if(url.pathname==='/api/config') return send(200,JSON.stringify({app:'SignalFlow Connect',mode:'demo',destinations:destinations()}),'application/json; charset=utf-8');
  if(url.pathname==='/' || url.pathname==='/index.html') return send(200,html,'text/html; charset=utf-8');
  return send(404,'Not found','text/plain');
});
server.listen(port,'0.0.0.0',()=>console.log('SignalFlow Connect prototype listening on '+port));
