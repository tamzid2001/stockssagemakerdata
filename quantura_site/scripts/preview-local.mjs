// Local visual verification server. Provider GETs use production read APIs;
// mutation requests are disabled so previewing cannot start paid compute.
import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
const root=path.resolve(new URL('..',import.meta.url).pathname);
const port = Number(process.env.QUANTURA_PREVIEW_PORT || 4184);
http.createServer(async(req,res)=>{
  try {
    const url=new URL(req.url,'http://localhost:4173');
    if (req.method!=='GET' && req.method!=='HEAD') {res.writeHead(405);res.end('Local preview is read only');return;}
    if(url.pathname.startsWith('/api/')||url.pathname.startsWith('/__/')){
      const upstream=await fetch('https://quantura.studio'+url.pathname+url.search,{headers:req.headers.authorization?{authorization:req.headers.authorization}:{}});
      res.writeHead(upstream.status,{'Content-Type':upstream.headers.get('content-type')||'application/json'});
      res.end(Buffer.from(await upstream.arrayBuffer()));return;
    }
    let relative=decodeURIComponent(url.pathname).replace(/^\//,'')||'index.html';
    if(relative.includes('..')){res.writeHead(400);res.end();return;}
    if(!path.extname(relative))relative+='.html';
    let file=path.join(root,'public',relative);
    if(!await fs.stat(file).catch(()=>null))file=path.join(root,'pages',relative);
    const type={'.js':'text/javascript','.css':'text/css','.html':'text/html','.svg':'image/svg+xml','.webp':'image/webp','.png':'image/png','.json':'application/json','.woff2':'font/woff2'}[path.extname(file)]||'application/octet-stream';
    const bytes=await fs.readFile(file);
    res.writeHead(200,{'Content-Type':type,'Cache-Control':'no-store'});res.end(bytes);
  } catch {if(!res.headersSent)res.writeHead(404);res.end('Not found');}
}).listen(port,'127.0.0.1',()=>console.log(`Read-only preview on http://127.0.0.1:${port}`));
