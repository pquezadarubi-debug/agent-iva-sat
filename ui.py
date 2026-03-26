# -*- coding: utf-8 -*-
"""
ui.py — Interfaz visual del Agente IVA Devoluciones SAT
Usa http.server (stdlib) + navegador del sistema Windows.
Sin dependencias externas (funciona con Python 3.9+).
"""

import os
import sys
import json
import subprocess
import threading
import time
import datetime
import webbrowser
import socket
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

# Forzar UTF-8 en stdout
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─── Directorio base ───────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent

# ─── Estado global del servidor ───────────────────────────────────────────
_estado = {
    "procesando": False,
    "archivos": {},           # tipo → ruta Path
    "progreso": [],           # log de líneas de progreso
    "resultado": None,        # dict con totales
    "proc": None,             # subprocess activo
}

# ─── HTML de la interfaz ───────────────────────────────────────────────────
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Agente IVA — Devoluciones SAT</title>
<style>
  :root {
    --azul-header:#1F4E79; --azul-btn:#2E75B6; --azul-hover:#1F4E79;
    --verde-ok-bg:#E2EFDA; --verde-ok-fg:#375623;
    --amber-bg:#FFF2CC;    --amber-fg:#7F6000;
    --rojo-bg:#FCE4D6;     --rojo-fg:#C00000;
    --borde:#D0D0D0;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:'Segoe UI',Arial,sans-serif;font-size:13px;background:#fff;
       color:#222;height:100vh;display:flex;flex-direction:column;overflow:hidden}
  header{background:var(--azul-header);color:#fff;padding:10px 20px;
         display:flex;align-items:center;gap:12px;flex-shrink:0}
  header .logo{font-size:20px;font-weight:700;letter-spacing:1px}
  header .sub{font-size:11px;opacity:.8}
  .tabs-bar{background:var(--azul-header);display:flex;padding:0 16px;flex-shrink:0}
  .tab{color:rgba(255,255,255,.65);padding:8px 18px;cursor:pointer;
       border-bottom:3px solid transparent;font-size:12px;font-weight:500;
       transition:all .15s;user-select:none}
  .tab:hover{color:#fff}
  .tab.active{color:#fff;border-bottom-color:#70B0E0;background:rgba(255,255,255,.08)}
  .content{flex:1;overflow-y:auto;padding:18px 22px}
  .panel{display:none}.panel.active{display:block}
  .card{background:#fff;border:1px solid var(--borde);border-radius:6px;
        padding:14px 16px;margin-bottom:14px}
  .card-title{font-weight:600;font-size:12px;color:var(--azul-header);
              text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;
              border-bottom:1px solid var(--borde);padding-bottom:6px}
  .file-row{display:flex;align-items:center;padding:8px 0;
            border-bottom:1px solid #f0f0f0;gap:10px}
  .file-row:last-child{border-bottom:none}
  .file-icon{font-size:18px;width:26px;text-align:center}
  .file-label{flex:1;font-size:13px}
  .file-badge{background:#E8F0FE;color:#1a56db;border-radius:12px;
              padding:2px 10px;font-size:11px;font-weight:600;
              min-width:60px;text-align:center}
  .file-badge.ok{background:var(--verde-ok-bg);color:var(--verde-ok-fg)}
  .file-badge.err{background:var(--rojo-bg);color:var(--rojo-fg)}
  .file-badge.warn{background:var(--amber-bg);color:var(--amber-fg)}
  .cfg-status{display:flex;align-items:center;gap:8px;padding:8px 12px;
              border-radius:4px;font-size:12px;margin-bottom:12px}
  .cfg-status.ok{background:var(--verde-ok-bg);color:var(--verde-ok-fg)}
  .cfg-status.fail{background:var(--rojo-bg);color:var(--rojo-fg)}
  .btn{background:var(--azul-btn);color:#fff;border:none;border-radius:4px;
       padding:8px 16px;cursor:pointer;font-size:12px;font-weight:500;
       transition:background .15s}
  .btn:hover{background:var(--azul-hover)}
  .btn:active{transform:scale(.98)}
  .btn.secondary{background:#fff;color:var(--azul-btn);border:1px solid var(--azul-btn)}
  .btn.secondary:hover{background:#EBF3FC}
  .btn.grande{font-size:14px;padding:11px 28px;font-weight:600}
  .btn.danger{background:#C00000}
  .btn.danger:hover{background:#a00000}
  .btn-row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
  .p-item{display:flex;align-items:center;gap:10px;padding:7px 0;
          border-bottom:1px solid #f5f5f5}
  .p-dot{width:12px;height:12px;border-radius:50%;flex-shrink:0;
         background:#ccc;transition:background .3s}
  .p-dot.ok{background:#4CAF50}
  .p-dot.run{background:var(--azul-btn);animation:pulse .9s infinite}
  .p-dot.err{background:#C00000}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
  .p-label{width:200px;font-size:12px}
  .p-bar-wrap{flex:1;height:8px;background:#e8e8e8;border-radius:4px;overflow:hidden}
  .p-bar{height:100%;background:var(--azul-btn);border-radius:4px;
         width:0%;transition:width .4s}
  .p-bar.ok{background:#4CAF50}.p-bar.err{background:#C00000}
  .p-count{width:140px;font-size:11px;color:#666;text-align:right}
  .log-box{background:#1E1E1E;color:#D4D4D4;font-family:'Consolas',monospace;
           font-size:11px;padding:10px 12px;height:160px;overflow-y:auto;
           border-radius:4px;margin-top:12px;line-height:1.6}
  .log-box .ok{color:#6EDB6E}.log-box .warn{color:#E5C07B}.log-box .err{color:#E06C75}
  .metrics-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:14px}
  .metric-card{border-radius:6px;padding:14px 16px;border-left:4px solid transparent}
  .metric-card.verde{background:var(--verde-ok-bg);border-left-color:#4CAF50}
  .metric-card.amber{background:var(--amber-bg);border-left-color:#FFC107}
  .metric-card.rojo{background:var(--rojo-bg);border-left-color:#C00000}
  .metric-valor{font-size:22px;font-weight:700;margin-bottom:4px}
  .verde .metric-valor{color:var(--verde-ok-fg)}
  .amber .metric-valor{color:var(--amber-fg)}
  .rojo  .metric-valor{color:var(--rojo-fg)}
  .metric-label{font-size:11px;color:#555}
  .ent-row{display:flex;align-items:center;gap:10px;padding:10px 0;
           border-bottom:1px solid #f0f0f0}
  .ent-row:last-child{border-bottom:none}
  .ent-icon{font-size:22px;width:30px;text-align:center}
  .ent-name{flex:1;font-size:13px;font-weight:500}
  .ent-st{font-size:11px;padding:2px 8px;border-radius:10px;
          background:#f0f0f0;color:#666}
  .ent-st.ok{background:var(--verde-ok-bg);color:var(--verde-ok-fg)}
  .alert-banner{background:var(--amber-bg);border:1px solid #FFCA28;
                border-radius:4px;padding:9px 14px;font-size:12px;
                color:var(--amber-fg);display:flex;align-items:center;
                gap:8px;margin-top:8px}
  .hidden{display:none!important}
</style>
</head>
<body>
<header>
  <div>
    <div class="logo">&#128202; AGENTE IVA</div>
    <div class="sub">Devoluciones SAT &mdash; Mexico</div>
  </div>
</header>
<div class="tabs-bar">
  <div class="tab active" onclick="showTab('archivos')"    id="tab-archivos">&#128193; Archivos</div>
  <div class="tab"        onclick="showTab('procesando')"  id="tab-procesando">&#9881;&#65039; Procesando</div>
  <div class="tab"        onclick="showTab('resultados')"  id="tab-resultados">&#128200; Resultados</div>
  <div class="tab"        onclick="showTab('entregables')" id="tab-entregables">&#128230; Entregables</div>
</div>
<div class="content">

<!-- PESTANA 1 -->
<div class="panel active" id="panel-archivos">
  <div id="cfg-box" class="cfg-status ok">
    <span id="cfg-dot">&#128994;</span>
    <span id="cfg-msg">Verificando config.json...</span>
  </div>
  <div class="card">
    <div class="card-title">Archivos de entrada detectados</div>
    <div class="file-row">
      <span class="file-icon">&#128196;</span>
      <span class="file-label">CFDIs Complemento de Pago (XML)</span>
      <span class="file-badge" id="b-cfdi">—</span>
    </div>
    <div class="file-row">
      <span class="file-icon">&#128203;</span>
      <span class="file-label">Estado de cuenta bancario (PDF)</span>
      <span class="file-badge" id="b-pdf">—</span>
    </div>
    <div class="file-row">
      <span class="file-icon">&#128202;</span>
      <span class="file-label">Auxiliar SAP (Excel)</span>
      <span class="file-badge" id="b-sap">—</span>
    </div>
    <div class="file-row">
      <span class="file-icon">&#128221;</span>
      <span class="file-label">Machote Word (opcional)</span>
      <span class="file-badge" id="b-doc">—</span>
    </div>
  </div>
  <div class="btn-row">
    <button class="btn secondary" onclick="api('abrir_input')">&#128194; Abrir carpeta input</button>
    <button class="btn grande" id="btn-proc" onclick="iniciar()">&#9654; PROCESAR AHORA</button>
  </div>
</div>

<!-- PESTANA 2 -->
<div class="panel" id="panel-procesando">
  <div class="card">
    <div class="card-title">Progreso de procesamiento</div>
    <div class="p-item"><div class="p-dot" id="d-cfdi"></div><div class="p-label">Parseo de CFDIs XML</div><div class="p-bar-wrap"><div class="p-bar" id="br-cfdi"></div></div><div class="p-count" id="c-cfdi">—</div></div>
    <div class="p-item"><div class="p-dot" id="d-estado_cuenta"></div><div class="p-label">Lectura estado de cuenta</div><div class="p-bar-wrap"><div class="p-bar" id="br-estado_cuenta"></div></div><div class="p-count" id="c-estado_cuenta">—</div></div>
    <div class="p-item"><div class="p-dot" id="d-cruce_banco"></div><div class="p-label">Cruce con banco</div><div class="p-bar-wrap"><div class="p-bar" id="br-cruce_banco"></div></div><div class="p-count" id="c-cruce_banco">—</div></div>
    <div class="p-item"><div class="p-dot" id="d-auxiliar_sap"></div><div class="p-label">Cruce con auxiliar SAP</div><div class="p-bar-wrap"><div class="p-bar" id="br-auxiliar_sap"></div></div><div class="p-count" id="c-auxiliar_sap">—</div></div>
    <div class="p-item"><div class="p-dot" id="d-excel"></div><div class="p-label">Generacion Excel reporte</div><div class="p-bar-wrap"><div class="p-bar" id="br-excel"></div></div><div class="p-count" id="c-excel">—</div></div>
    <div class="p-item"><div class="p-dot" id="d-pdf"></div><div class="p-label">Marcado PDF</div><div class="p-bar-wrap"><div class="p-bar" id="br-pdf"></div></div><div class="p-count" id="c-pdf">—</div></div>
    <div class="p-item"><div class="p-dot" id="d-auxiliar_cruzado"></div><div class="p-label">Auxiliar SAP cruzado</div><div class="p-bar-wrap"><div class="p-bar" id="br-auxiliar_cruzado"></div></div><div class="p-count" id="c-auxiliar_cruzado">—</div></div>
    <div class="p-item"><div class="p-dot" id="d-word"></div><div class="p-label">Escrito Word SAT</div><div class="p-bar-wrap"><div class="p-bar" id="br-word"></div></div><div class="p-count" id="c-word">—</div></div>
  </div>
  <div class="card">
    <div class="card-title">Log en tiempo real</div>
    <div class="log-box" id="log-box"><span class="warn">Esperando inicio...</span></div>
  </div>
</div>

<!-- PESTANA 3 -->
<div class="panel" id="panel-resultados">
  <div class="metrics-grid">
    <div class="metric-card verde"><div class="metric-valor" id="m-iva">$0.00</div><div class="metric-label">IVA acreditable confirmado</div></div>
    <div class="metric-card verde"><div class="metric-valor" id="m-cruces">0 / 0</div><div class="metric-label">CFDIs con cruce completo</div></div>
    <div class="metric-card amber"><div class="metric-valor" id="m-parcial">0</div><div class="metric-label">Cruce parcial &mdash; revisar</div></div>
    <div class="metric-card rojo"><div class="metric-valor"  id="m-sinc">0</div><div class="metric-label">Sin cruce &mdash; accion requerida</div></div>
  </div>
  <div id="alert-sinc" class="alert-banner hidden">
    &#9888;&#65039; <strong>Atencion:</strong> hay movimientos sin cruce que requieren revision manual.
  </div>
  <div class="card">
    <div class="card-title">Resumen</div>
    <div id="resumen" style="font-size:12px;color:#444;line-height:1.8">Aun no se ha procesado ningun periodo.</div>
  </div>
</div>

<!-- PESTANA 4 -->
<div class="panel" id="panel-entregables">
  <div class="card">
    <div class="card-title">Archivos generados</div>
    <div class="ent-row">
      <span class="ent-icon">&#128202;</span>
      <span class="ent-name" id="en-excel">reporte_iva_YYYYMM.xlsx</span>
      <span class="ent-st" id="es-excel">Pendiente</span>
      <button class="btn secondary" style="padding:4px 10px;font-size:11px" onclick="api('abrir_excel')">Abrir</button>
      <button class="btn secondary" style="padding:4px 10px;font-size:11px" onclick="api('abrir_output')">&#128194;</button>
    </div>
    <div class="ent-row">
      <span class="ent-icon">&#128203;</span>
      <span class="ent-name" id="en-pdf">estado_cuenta_cruzado_YYYYMM.pdf</span>
      <span class="ent-st" id="es-pdf">Pendiente</span>
      <button class="btn secondary" style="padding:4px 10px;font-size:11px" onclick="api('abrir_pdf')">Abrir</button>
      <button class="btn secondary" style="padding:4px 10px;font-size:11px" onclick="api('abrir_output')">&#128194;</button>
    </div>
    <div class="ent-row">
      <span class="ent-icon">&#128200;</span>
      <span class="ent-name" id="en-sap">auxiliar_sap_cruzado_YYYYMM.xlsx</span>
      <span class="ent-st" id="es-sap">Pendiente</span>
      <button class="btn secondary" style="padding:4px 10px;font-size:11px" onclick="api('abrir_sap')">Abrir</button>
      <button class="btn secondary" style="padding:4px 10px;font-size:11px" onclick="api('abrir_output')">&#128194;</button>
    </div>
    <div class="ent-row">
      <span class="ent-icon">&#128221;</span>
      <span class="ent-name" id="en-word">escrito_devolucion_IVA_YYYYMM.docx</span>
      <span class="ent-st" id="es-word">Pendiente</span>
      <button class="btn secondary" style="padding:4px 10px;font-size:11px" onclick="api('abrir_word')">Abrir</button>
      <button class="btn secondary" style="padding:4px 10px;font-size:11px" onclick="api('abrir_output')">&#128194;</button>
    </div>
  </div>
  <div class="btn-row">
    <button class="btn danger" onclick="nuevoPeriodo()">&#128260; Nuevo periodo</button>
  </div>
</div>
</div>

<script>
let encuesta = null;

function showTab(n){
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById('panel-'+n).classList.add('active');
  document.getElementById('tab-'+n).classList.add('active');
}

async function api(accion, extra){
  try{
    const url = '/api?a='+accion+(extra?'&'+extra:'');
    const r = await fetch(url);
    return await r.json();
  }catch(e){ console.error(e); return {}; }
}

function badge(id, n, opcional){
  const el = document.getElementById(id);
  if(n===0){el.textContent=opcional?'Opcional':'0 archivos';el.className='file-badge '+(opcional?'warn':'err');}
  else if(n===true){el.textContent='Encontrado';el.className='file-badge ok';}
  else if(n===false){el.textContent='No encontrado';el.className='file-badge '+(opcional?'warn':'err');}
  else{el.textContent=n+' archivo'+(n===1?'':'s');el.className='file-badge ok';}
}

function updProg(paso,pct,msg){
  const d=document.getElementById('d-'+paso);
  const b=document.getElementById('br-'+paso);
  const c=document.getElementById('c-'+paso);
  if(!d)return;
  const p=parseInt(pct);
  b.style.width=p+'%';
  if(p>=100){d.className='p-dot ok';b.className='p-bar ok';}
  else{d.className='p-dot run';b.className='p-bar';}
  if(c) c.textContent=msg.length>24?msg.substring(0,24)+'...':msg;
  log(msg, p>=100?'ok':'');
}

function log(msg, cls){
  const box=document.getElementById('log-box');
  const el=document.createElement('div');
  const t=new Date().toLocaleTimeString('es-MX',{hour12:false});
  el.textContent='['+t+'] '+msg;
  if(cls) el.className=cls;
  box.appendChild(el);
  box.scrollTop=box.scrollHeight;
}

async function cargarEstado(){
  const d = await api('estado');
  if(!d) return;
  badge('b-cfdi', d.cfdi);
  badge('b-pdf',  d.pdf);
  badge('b-sap',  d.sap);
  badge('b-doc',  d.machote, true);
  const cfg=document.getElementById('cfg-box');
  const msg=document.getElementById('cfg-msg');
  const dot=document.getElementById('cfg-dot');
  if(d.cfg_ok){cfg.className='cfg-status ok';dot.textContent='&#128994;';}
  else{cfg.className='cfg-status fail';dot.textContent='&#128308;';}
  msg.innerHTML=d.cfg_msg||'';
}

function mostrarResultado(d){
  const total=parseInt(d.total)||0;
  const iva=parseFloat(d.iva)||0;
  const ok=parseInt(d.cruces)||0;
  const sinc=parseInt(d.sin_cruce)||0;
  const parc=Math.max(0,total-ok-sinc);
  document.getElementById('m-iva').textContent='$'+iva.toLocaleString('es-MX',{minimumFractionDigits:2});
  document.getElementById('m-cruces').textContent=ok+' / '+total+(total>0?' ('+Math.round(ok/total*100)+'%)':'');
  document.getElementById('m-parcial').textContent=parc;
  document.getElementById('m-sinc').textContent=sinc;
  if(sinc>0) document.getElementById('alert-sinc').classList.remove('hidden');
  document.getElementById('resumen').innerHTML=
    '<b>CFDIs:</b> '+total+' &nbsp;|&nbsp; <b>IVA acreditable:</b> $'+iva.toLocaleString('es-MX',{minimumFractionDigits:2})+
    ' &nbsp;|&nbsp; <b>Cruce completo:</b> '+ok+' &nbsp;|&nbsp; <b>Parcial:</b> '+parc+' &nbsp;|&nbsp; <b>Sin cruce:</b> '+sinc;
}

async function iniciar(){
  if(document.getElementById('btn-proc').disabled) return;
  document.getElementById('btn-proc').disabled=true;
  document.getElementById('btn-proc').textContent='... Procesando';
  document.getElementById('log-box').innerHTML='';
  ['cfdi','estado_cuenta','cruce_banco','auxiliar_sap','excel','pdf','auxiliar_cruzado','word'].forEach(p=>{
    const d=document.getElementById('d-'+p);
    const b=document.getElementById('br-'+p);
    const c=document.getElementById('c-'+p);
    if(d)d.className='p-dot';
    if(b){b.className='p-bar';b.style.width='0%';}
    if(c)c.textContent='—';
  });
  showTab('procesando');
  await api('procesar');
  // Iniciar polling de progreso
  encuesta = setInterval(pollProgreso, 800);
}

async function pollProgreso(){
  const d = await api('progreso');
  if(!d || !d.lineas) return;
  d.lineas.forEach(linea=>{
    if(linea.startsWith('PROGRESO:')){
      const p=linea.split(':'); if(p.length>=4) updProg(p[1],p[2],p[3]);
    } else if(linea.startsWith('RESULTADO:')){
      const p=linea.split(':');
      if(p.length>=5){
        clearInterval(encuesta); encuesta=null;
        mostrarResultado({total:p[1],iva:p[2],cruces:p[3],sin_cruce:p[4]});
        document.getElementById('btn-proc').disabled=false;
        document.getElementById('btn-proc').textContent='▶ PROCESAR AHORA';
        // Actualizar entregables
        actualizarEntregables();
        showTab('entregables');
      }
    } else if(linea.startsWith('ERROR:')){
      log(linea.substring(6),'err');
      clearInterval(encuesta); encuesta=null;
      document.getElementById('btn-proc').disabled=false;
      document.getElementById('btn-proc').textContent='▶ PROCESAR AHORA';
    } else if(linea.trim()){
      log(linea,'');
    }
  });
}

async function actualizarEntregables(){
  const d = await api('archivos_output');
  if(!d) return;
  ['excel','pdf','sap','word'].forEach(k=>{
    if(d[k]){
      document.getElementById('en-'+k).textContent=d[k];
      const st=document.getElementById('es-'+k);
      st.textContent='Generado OK';
      st.className='ent-st ok';
    }
  });
}

async function nuevoPeriodo(){
  if(!confirm('Limpiar output/ y comenzar nuevo periodo?')) return;
  await api('nuevo_periodo');
  ['excel','pdf','sap','word'].forEach(k=>{
    document.getElementById('es-'+k).textContent='Pendiente';
    document.getElementById('es-'+k).className='ent-st';
  });
  document.getElementById('btn-proc').disabled=false;
  document.getElementById('btn-proc').textContent='▶ PROCESAR AHORA';
  showTab('archivos');
}

// Carga inicial
cargarEstado();
setInterval(cargarEstado, 5000);
</script>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════════════════════════
# Servidor HTTP que expone API JSON
# ══════════════════════════════════════════════════════════════════════════════

_log_buffer = []       # líneas de stdout del subprocess
_log_lock   = threading.Lock()


class AgentHandler(BaseHTTPRequestHandler):
    """Maneja peticiones GET /  y  /api?a=accion"""

    def log_message(self, fmt, *args):
        pass  # silenciar logs de acceso en consola

    def _json(self, data: dict, code: int = 200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _html(self, html: str):
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._html(HTML_PAGE)
            return

        if parsed.path == "/api":
            params = parse_qs(parsed.query)
            accion = params.get("a", [""])[0]
            self._manejar(accion)
            return

        self.send_response(404)
        self.end_headers()

    def _manejar(self, accion: str):
        global _estado, _log_buffer

        # ── Estado inicial (archivos) ──────────────────────────────────────
        if accion == "estado":
            cfdi_dir = BASE_DIR / "input" / "cfdi"
            pdf_dir  = BASE_DIR / "input" / "estado_cuenta"
            sap_dir  = BASE_DIR / "input" / "auxiliar"
            doc_dir  = BASE_DIR / "input" / "machote"

            n_cfdi = len(list(cfdi_dir.glob("*.xml")) + list(cfdi_dir.glob("*.XML")))
            n_pdf  = len(list(pdf_dir.glob("*.pdf"))  + list(pdf_dir.glob("*.PDF")))
            n_sap  = (len(list(sap_dir.glob("*.xlsx"))) +
                      len(list(sap_dir.glob("*.xls")))  +
                      len(list(sap_dir.glob("*.XLSX"))))
            machote = bool(list(doc_dir.glob("*.docx")))

            cfg_ok  = False
            cfg_msg = "config.json no encontrado"
            cfg_path = BASE_DIR / "input" / "config.json"
            if cfg_path.exists():
                try:
                    with open(cfg_path, encoding="utf-8") as f:
                        cfg = json.load(f)
                    faltan = [k for k in ["empresa", "rfc", "domicilio", "clabe",
                                          "rep_legal", "rfc_rep"]
                              if not cfg.get(k, "").strip()]
                    if faltan:
                        cfg_msg = f"Faltan en config.json: {', '.join(faltan)}"
                    else:
                        cfg_ok  = True
                        cfg_msg = f"config.json OK — {cfg.get('empresa','')}"
                except Exception as e:
                    cfg_msg = f"Error leyendo config.json: {e}"

            self._json({"cfdi": n_cfdi, "pdf": n_pdf, "sap": n_sap,
                        "machote": machote, "cfg_ok": cfg_ok, "cfg_msg": cfg_msg})
            return

        # ── Iniciar procesamiento ──────────────────────────────────────────
        if accion == "procesar":
            if _estado["procesando"]:
                self._json({"ok": False, "msg": "Ya procesando"})
                return
            _estado["procesando"] = True
            with _log_lock:
                _log_buffer.clear()

            def _run():
                global _log_buffer
                agente = BASE_DIR / "agente_iva.py"
                cmd = [sys.executable, str(agente), str(BASE_DIR)]
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                )
                _estado["proc"] = proc
                for linea in proc.stdout:
                    linea = linea.rstrip()
                    if linea:
                        with _log_lock:
                            _log_buffer.append(linea)
                proc.wait()
                _estado["procesando"] = False
                _estado["proc"] = None

            threading.Thread(target=_run, daemon=True).start()
            self._json({"ok": True})
            return

        # ── Polling de progreso ────────────────────────────────────────────
        if accion == "progreso":
            with _log_lock:
                lineas = _log_buffer.copy()
                _log_buffer.clear()
            self._json({"lineas": lineas, "procesando": _estado["procesando"]})
            return

        # ── Archivos generados en output/ ──────────────────────────────────
        if accion == "archivos_output":
            out = BASE_DIR / "output"
            res = {}
            mapeo = {
                "excel": ("reporte_iva_", ".xlsx"),
                "pdf":   ("estado_cuenta_", ".pdf"),
                "sap":   ("auxiliar_sap_", ".xlsx"),
                "word":  ("escrito_devolucion_", ".docx"),
            }
            for key, (pre, ext) in mapeo.items():
                for f in out.glob(f"{pre}*{ext}"):
                    res[key] = f.name
                    break
            self._json(res)
            return

        # ── Nuevo periodo ──────────────────────────────────────────────────
        if accion == "nuevo_periodo":
            import shutil
            out = BASE_DIR / "output"
            if out.exists():
                for item in out.iterdir():
                    try:
                        item.unlink() if item.is_file() else shutil.rmtree(item)
                    except Exception:
                        pass
            self._json({"ok": True})
            return

        # ── Abrir archivo / carpeta ────────────────────────────────────────
        if accion == "abrir_input":
            os.startfile(str(BASE_DIR / "input"))
            self._json({"ok": True})
            return

        if accion == "abrir_output":
            ruta = BASE_DIR / "output"
            ruta.mkdir(exist_ok=True)
            os.startfile(str(ruta))
            self._json({"ok": True})
            return

        for tipo, (pre, ext) in [
            ("excel", ("reporte_iva_", ".xlsx")),
            ("pdf",   ("estado_cuenta_", ".pdf")),
            ("sap",   ("auxiliar_sap_", ".xlsx")),
            ("word",  ("escrito_devolucion_", ".docx")),
        ]:
            if accion == f"abrir_{tipo}":
                for f in (BASE_DIR / "output").glob(f"{pre}*{ext}"):
                    os.startfile(str(f))
                    break
                self._json({"ok": True})
                return

        self._json({"error": f"accion desconocida: {accion}"}, 400)


def _puerto_libre() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def main():
    puerto = _puerto_libre()
    url    = f"http://127.0.0.1:{puerto}"

    servidor = HTTPServer(("127.0.0.1", puerto), AgentHandler)
    hilo = threading.Thread(target=servidor.serve_forever, daemon=True)
    hilo.start()

    print(f"[AgentIVA] Servidor iniciado en {url}", flush=True)
    # Esperar un instante y abrir navegador
    time.sleep(0.5)
    webbrowser.open(url)

    # Mantener proceso vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        servidor.shutdown()


if __name__ == "__main__":
    main()
