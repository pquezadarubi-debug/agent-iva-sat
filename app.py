# -*- coding: utf-8 -*-
"""
app.py — AgentIVA Devoluciones SAT — versión web (Flask)
Cualquier persona con la URL puede subir sus archivos y procesar su IVA.
Sesiones aisladas por UUID, archivos temporales borrados tras 2 horas.
"""

import os
import sys
import json
import uuid
import shutil
import threading
import time
import datetime
import subprocess
from pathlib import Path

from flask import (Flask, request, Response, send_file,
                   jsonify, make_response, stream_with_context)
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(32))

# ─── Directorio de sesiones ────────────────────────────────────────────────
# En Render /tmp es efímero pero suficiente para procesamiento
SESSIONS_DIR = Path(os.environ.get("SESSIONS_DIR", "/tmp/agentiva"))
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# Ruta al motor principal (mismo directorio que app.py)
AGENTE_PY = Path(__file__).parent / "agente_iva.py"

# ─── Estado de procesos activos {sid: subprocess} ─────────────────────────
_procs: dict[str, subprocess.Popen] = {}
_procs_lock = threading.Lock()


# ══════════════════════════════════════════════════════════════════════════
# Helpers de sesión
# ══════════════════════════════════════════════════════════════════════════

def _sid() -> str:
    """Lee o crea el ID de sesión desde la cookie."""
    sid = request.cookies.get("sid", "")
    if not sid or len(sid) != 36:
        sid = str(uuid.uuid4())
    return sid


def _set_sid(response: Response, sid: str) -> Response:
    response.set_cookie("sid", sid, max_age=7 * 24 * 3600, samesite="Lax")
    return response


def _session_dir(sid: str) -> Path:
    d = SESSIONS_DIR / sid
    for sub in ["input/cfdi", "input/cfdi_cobro", "input/estado_cuenta",
                "input/auxiliar", "input/machote", "output"]:
        (d / sub).mkdir(parents=True, exist_ok=True)
    return d


def _limpiar_sesiones_antiguas():
    """Borra sesiones con más de 2 horas de antigüedad."""
    while True:
        time.sleep(1800)  # cada 30 minutos
        ahora = time.time()
        if not SESSIONS_DIR.exists():
            continue
        for d in SESSIONS_DIR.iterdir():
            try:
                if d.is_dir() and (ahora - d.stat().st_mtime) > 7200:
                    shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass


threading.Thread(target=_limpiar_sesiones_antiguas, daemon=True).start()


# ══════════════════════════════════════════════════════════════════════════
# HTML de la interfaz (embebido)
# ══════════════════════════════════════════════════════════════════════════

HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Agente IVA &mdash; Devoluciones SAT</title>
<style>
:root{
  --az:#1F4E79;--azb:#2E75B6;--azh:#1F4E79;
  --vbg:#E2EFDA;--vfg:#375623;
  --abg:#FFF2CC;--afg:#7F6000;
  --rbg:#FCE4D6;--rfg:#C00000;
  --borde:#D0D0D0;
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',Arial,sans-serif;font-size:13px;
     display:flex;flex-direction:column;height:100vh;overflow:hidden}
header{background:var(--az);color:#fff;padding:10px 20px;
       display:flex;align-items:center;gap:16px;flex-shrink:0}
.logo{font-size:18px;font-weight:700}
.sub{font-size:11px;opacity:.75}
.badge-pub{background:rgba(255,255,255,.2);padding:2px 10px;
           border-radius:10px;font-size:11px;margin-left:auto}
.tabs{background:var(--az);display:flex;padding:0 16px;flex-shrink:0}
.tab{color:rgba(255,255,255,.6);padding:8px 18px;cursor:pointer;
     border-bottom:3px solid transparent;font-size:12px;font-weight:500;
     user-select:none;transition:color .15s}
.tab:hover{color:#fff}
.tab.active{color:#fff;border-bottom-color:#70B0E0;background:rgba(255,255,255,.08)}
.content{flex:1;overflow-y:auto;padding:16px 20px}
.panel{display:none}.panel.active{display:block}
.card{background:#fff;border:1px solid var(--borde);border-radius:6px;
      padding:14px 16px;margin-bottom:14px}
.ctitle{font-weight:600;font-size:11px;color:var(--az);text-transform:uppercase;
        letter-spacing:.5px;margin-bottom:10px;border-bottom:1px solid var(--borde);
        padding-bottom:6px}
/* Upload zones */
.upload-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px}
.uzone{border:2px dashed var(--borde);border-radius:6px;padding:14px 12px;
       text-align:center;cursor:pointer;transition:all .15s;background:#fafafa;
       position:relative}
.uzone:hover,.uzone.drag{border-color:var(--azb);background:#EBF3FC}
.uzone.ok{border-color:#4CAF50;background:var(--vbg)}
.uzone input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}
.uicon{font-size:24px;margin-bottom:4px}
.ulabel{font-size:12px;font-weight:600;color:#333;margin-bottom:2px}
.usub{font-size:10px;color:#888}
.ust{font-size:11px;margin-top:6px;font-weight:500}
.uzone.ok .ust{color:var(--vfg)}
.uzone:not(.ok) .ust{color:#aaa}
/* Config form */
.cfg-form{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.fld{display:flex;flex-direction:column;gap:3px}
.fld label{font-size:11px;font-weight:600;color:#555}
.fld input{border:1px solid var(--borde);border-radius:4px;padding:6px 8px;
           font-size:12px;font-family:inherit}
.fld input:focus{outline:none;border-color:var(--azb)}
.fld.full{grid-column:1/-1}
/* Buttons */
.btn{background:var(--azb);color:#fff;border:none;border-radius:4px;
     padding:8px 16px;cursor:pointer;font-size:12px;font-weight:500;
     transition:background .15s}
.btn:hover{background:var(--azh)}
.btn:disabled{opacity:.5;cursor:default}
.btn.sec{background:#fff;color:var(--azb);border:1px solid var(--azb)}
.btn.sec:hover{background:#EBF3FC}
.btn.big{font-size:14px;padding:11px 28px;font-weight:700}
.btn.danger{background:#C00000}.btn.danger:hover{background:#a00000}
.btn-row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-top:6px}
/* Progress */
.pit{display:flex;align-items:center;gap:10px;padding:7px 0;
     border-bottom:1px solid #f5f5f5}
.pdot{width:12px;height:12px;border-radius:50%;background:#ccc;flex-shrink:0}
.pdot.ok{background:#4CAF50}
.pdot.run{background:var(--azb);animation:p .9s infinite}
.pdot.err{background:#C00000}
@keyframes p{0%,100%{opacity:1}50%{opacity:.3}}
.plbl{width:200px;font-size:12px}
.pbw{flex:1;height:8px;background:#e8e8e8;border-radius:4px;overflow:hidden}
.pb{height:100%;background:var(--azb);border-radius:4px;width:0%;transition:width .4s}
.pb.ok{background:#4CAF50}.pb.err{background:#C00000}
.pcnt{width:140px;font-size:11px;color:#666;text-align:right}
.log{background:#1E1E1E;color:#D4D4D4;font-family:'Consolas',monospace;
     font-size:11px;padding:10px 12px;height:150px;overflow-y:auto;
     border-radius:4px;margin-top:12px;line-height:1.6}
.log .ok{color:#6EDB6E}.log .warn{color:#E5C07B}.log .err{color:#E06C75}
/* Metrics */
.mgrid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:14px}
.mc{border-radius:6px;padding:14px 16px;border-left:4px solid transparent}
.mc.v{background:var(--vbg);border-left-color:#4CAF50}
.mc.a{background:var(--abg);border-left-color:#FFC107}
.mc.r{background:var(--rbg);border-left-color:#C00000}
.mv{font-size:22px;font-weight:700;margin-bottom:4px}
.v .mv{color:var(--vfg)}.a .mv{color:var(--afg)}.r .mv{color:var(--rfg)}
.ml{font-size:11px;color:#555}
/* Entregables */
.er{display:flex;align-items:center;gap:10px;padding:10px 0;
    border-bottom:1px solid #f0f0f0}
.er:last-child{border-bottom:none}
.eico{font-size:22px;width:30px;text-align:center}
.enm{flex:1;font-size:13px;font-weight:500}
.est{font-size:11px;padding:2px 8px;border-radius:10px;
     background:#f0f0f0;color:#666}
.est.ok{background:var(--vbg);color:var(--vfg)}
.hidden{display:none!important}
.alert{background:var(--abg);border:1px solid #FFCA28;border-radius:4px;
       padding:9px 14px;font-size:12px;color:var(--afg);
       display:flex;align-items:center;gap:8px;margin-top:8px}
/* Responsive */
@media(max-width:600px){
  .upload-grid{grid-template-columns:1fr}
  .cfg-form{grid-template-columns:1fr}
  .mgrid{grid-template-columns:1fr}
}
</style>
</head>
<body>
<header>
  <div>
    <div class="logo">&#128202; Agente IVA</div>
    <div class="sub">Devoluciones SAT &mdash; Mexico</div>
  </div>
  <div class="badge-pub">&#127760; Uso publico &mdash; archivos se borran en 2 h</div>
</header>
<div class="tabs">
  <div class="tab active" onclick="showTab('archivos')"    id="tab-archivos">&#128193; Archivos</div>
  <div class="tab"        onclick="showTab('procesando')"  id="tab-procesando">&#9881;&#65039; Procesando</div>
  <div class="tab"        onclick="showTab('resultados')"  id="tab-resultados">&#128200; Resultados</div>
  <div class="tab"        onclick="showTab('entregables')" id="tab-entregables">&#128230; Entregables</div>
</div>
<div class="content">

<!-- TAB 1 ARCHIVOS -->
<div class="panel active" id="panel-archivos">

  <div class="card">
    <div class="ctitle">1 — Sube tus archivos</div>
    <div class="upload-grid">

      <!-- CFDIs de PAGO (IVA Acreditable) -->
      <div class="uzone" id="z-cfdi" ondragover="drag(event,'cfdi')"
           ondragleave="undrag('cfdi')" ondrop="drop(event,'cfdi')">
        <input type="file" multiple accept=".xml,.XML"
               onchange="subirMultiple(this,'cfdi')" id="inp-cfdi">
        <div class="uicon">&#128196;</div>
        <div class="ulabel">CFDIs de PAGO <span style="font-size:10px;color:#888">(IVA Acreditable)</span></div>
        <div class="usub">Emisor = proveedor &mdash; .xml (varios)</div>
        <div class="ust" id="st-cfdi">Sin archivos</div>
      </div>

      <!-- CFDIs de COBRO (IVA Trasladado) -->
      <div class="uzone" id="z-cfdi_cobro" ondragover="drag(event,'cfdi_cobro')"
           ondragleave="undrag('cfdi_cobro')" ondrop="drop(event,'cfdi_cobro')">
        <input type="file" multiple accept=".xml,.XML"
               onchange="subirMultiple(this,'cfdi_cobro')" id="inp-cfdi_cobro">
        <div class="uicon">&#128196;</div>
        <div class="ulabel">CFDIs de COBRO <span style="font-size:10px;color:#888">(IVA Trasladado)</span></div>
        <div class="usub">Emisor = tu empresa &mdash; .xml (varios)</div>
        <div class="ust" id="st-cfdi_cobro">Sin archivos</div>
      </div>

      <!-- Estado de cuenta PDF -->
      <div class="uzone" id="z-pdf" ondragover="drag(event,'pdf')"
           ondragleave="undrag('pdf')" ondrop="drop(event,'pdf')">
        <input type="file" multiple accept=".pdf,.PDF"
               onchange="subirMultiple(this,'pdf')" id="inp-pdf">
        <div class="uicon">&#128203;</div>
        <div class="ulabel">Estado(s) de cuenta bancario</div>
        <div class="usub">Arrastra o haz clic &mdash; .pdf (uno o varios)</div>
        <div class="ust" id="st-pdf">Sin archivos</div>
      </div>

      <!-- Auxiliar SAP -->
      <div class="uzone" id="z-sap" ondragover="drag(event,'sap')"
           ondragleave="undrag('sap')" ondrop="drop(event,'sap')">
        <input type="file" accept=".xlsx,.xls,.XLSX,.XLS"
               onchange="subirUno(this,'sap')" id="inp-sap">
        <div class="uicon">&#128202;</div>
        <div class="ulabel">Auxiliar SAP (IVA Acreditable)</div>
        <div class="usub">Arrastra o haz clic &mdash; .xlsx</div>
        <div class="ust" id="st-sap">Sin archivo</div>
      </div>

      <!-- Machote Word (opcional) -->
      <div class="uzone" id="z-machote" ondragover="drag(event,'machote')"
           ondragleave="undrag('machote')" ondrop="drop(event,'machote')">
        <input type="file" accept=".docx,.DOCX"
               onchange="subirUno(this,'machote')" id="inp-machote">
        <div class="uicon">&#128221;</div>
        <div class="ulabel">Machote Word <em>(opcional)</em></div>
        <div class="usub">Arrastra o haz clic &mdash; .docx</div>
        <div class="ust" id="st-machote">Opcional</div>
      </div>

    </div>
  </div>

  <div class="card">
    <div class="ctitle">2 — Datos de la empresa</div>
    <div class="cfg-form" id="cfg-form">
      <div class="fld"><label>Empresa (razon social)</label>
        <input id="c-empresa" placeholder="MEDICINAS Y MEDICAMENTOS NACIONALES SA DE CV"></div>
      <div class="fld"><label>RFC empresa</label>
        <input id="c-rfc" placeholder="MMN090225361"></div>
      <div class="fld full"><label>Domicilio fiscal</label>
        <input id="c-domicilio" placeholder="Antonio Dovali Jaime 70, Santa Fe, CDMX"></div>
      <div class="fld"><label>CLABE (18 digitos)</label>
        <input id="c-clabe" placeholder="012180001134960237" maxlength="18"></div>
      <div class="fld"><label>Representante legal</label>
        <input id="c-rep" placeholder="Nombre completo"></div>
      <div class="fld"><label>RFC representante</label>
        <input id="c-rfcrep" placeholder="XAXX010101000"></div>
      <div class="fld full"><label>Autorizados (nombres y RFCs, separados por coma)</label>
        <input id="c-aut" placeholder="Juan Perez PEPJ800101XXX, Maria Lopez LOPM750215YYY"></div>
      <div class="fld"><label>Folio SAT (si ya lo tienes)</label>
        <input id="c-folio" placeholder="Dejar vacio si no tienes aun"></div>
    </div>
    <div class="btn-row" style="margin-top:10px">
      <button class="btn sec" onclick="guardarConfig()">&#128190; Guardar datos</button>
      <span id="cfg-saved" class="hidden" style="color:var(--vfg);font-size:12px">&#10003; Guardado</span>
    </div>
  </div>

  <div class="btn-row">
    <button class="btn big" id="btn-proc" onclick="iniciar()">&#x25BA; PROCESAR AHORA</button>
    <span id="proc-msg" style="font-size:11px;color:#888"></span>
  </div>
</div>

<!-- TAB 2 PROCESANDO -->
<div class="panel" id="panel-procesando">
  <div class="card">
    <div class="ctitle">Progreso</div>
    <div class="pit"><div class="pdot" id="d-cfdi"></div><div class="plbl">Parseo de CFDIs XML</div><div class="pbw"><div class="pb" id="b-cfdi"></div></div><div class="pcnt" id="c-cfdi">—</div></div>
    <div class="pit"><div class="pdot" id="d-estado_cuenta"></div><div class="plbl">Lectura estado de cuenta</div><div class="pbw"><div class="pb" id="b-estado_cuenta"></div></div><div class="pcnt" id="c-estado_cuenta">—</div></div>
    <div class="pit"><div class="pdot" id="d-cruce_banco"></div><div class="plbl">Cruce con banco</div><div class="pbw"><div class="pb" id="b-cruce_banco"></div></div><div class="pcnt" id="c-cruce_banco">—</div></div>
    <div class="pit"><div class="pdot" id="d-auxiliar_sap"></div><div class="plbl">Cruce con auxiliar SAP</div><div class="pbw"><div class="pb" id="b-auxiliar_sap"></div></div><div class="pcnt" id="c-auxiliar_sap">—</div></div>
    <div class="pit"><div class="pdot" id="d-excel"></div><div class="plbl">Generacion Excel reporte</div><div class="pbw"><div class="pb" id="b-excel"></div></div><div class="pcnt" id="c-excel">—</div></div>
    <div class="pit"><div class="pdot" id="d-pdf"></div><div class="plbl">Marcado PDF</div><div class="pbw"><div class="pb" id="b-pdf"></div></div><div class="pcnt" id="c-pdf">—</div></div>
    <div class="pit"><div class="pdot" id="d-auxiliar_cruzado"></div><div class="plbl">Auxiliar SAP cruzado</div><div class="pbw"><div class="pb" id="b-auxiliar_cruzado"></div></div><div class="pcnt" id="c-auxiliar_cruzado">—</div></div>
    <div class="pit"><div class="pdot" id="d-word"></div><div class="plbl">Escrito Word SAT</div><div class="pbw"><div class="pb" id="b-word"></div></div><div class="pcnt" id="c-word">—</div></div>
  </div>
  <div class="card">
    <div class="ctitle">Log</div>
    <div class="log" id="logbox"><span class="warn">Esperando inicio...</span></div>
  </div>
</div>

<!-- TAB 3 RESULTADOS -->
<div class="panel" id="panel-resultados">
  <div class="mgrid">
    <div class="mc v"><div class="mv" id="m-iva">$0.00</div><div class="ml">SALDO A FAVOR A SOLICITAR</div></div>
    <div class="mc v"><div class="mv" id="m-cru">0 / 0</div><div class="ml">CFDIs con cruce completo</div></div>
    <div class="mc a"><div class="mv" id="m-par">0</div><div class="ml">Cruce parcial &mdash; revisar</div></div>
    <div class="mc r"><div class="mv" id="m-sin">0</div><div class="ml">Sin cruce &mdash; accion requerida</div></div>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:14px">
    <div class="mc" style="background:#EBF3FC;border-left:4px solid #2E75B6">
      <div class="mv" id="m-trasl" style="color:#1F4E79;font-size:18px">$0.00</div>
      <div class="ml">IVA Trasladado (cobrado)</div></div>
    <div class="mc" style="background:#FFF2CC;border-left:4px solid #FFC107">
      <div class="mv" id="m-acred" style="color:#7F6000;font-size:18px">$0.00</div>
      <div class="ml">IVA Acreditable (pagado)</div></div>
    <div class="mc v">
      <div class="mv" id="m-saldo" style="font-size:18px">$0.00</div>
      <div class="ml">Saldo a Favor = Cobrado &minus; Pagado</div></div>
  </div>
  <div id="al-sin" class="alert hidden">&#9888;&#65039; <strong>Atencion:</strong> hay movimientos sin cruce que requieren revision manual.</div>
  <div class="card">
    <div class="ctitle">Resumen</div>
    <div id="resumen" style="font-size:12px;color:#444;line-height:1.8">Aun no procesado.</div>
  </div>
</div>

<!-- TAB 4 ENTREGABLES -->
<div class="panel" id="panel-entregables">
  <div class="card">
    <div class="ctitle">Descargar archivos generados</div>
    <div class="er">
      <span class="eico">&#128202;</span>
      <span class="enm" id="en-excel">reporte_iva_YYYYMM.xlsx</span>
      <span class="est" id="es-excel">Pendiente</span>
      <a class="btn sec" id="dl-excel" href="/download/excel" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
    <div class="er">
      <span class="eico">&#128203;</span>
      <span class="enm" id="en-pdf">estado_cuenta_cruzado_YYYYMM.pdf</span>
      <span class="est" id="es-pdf">Pendiente</span>
      <a class="btn sec" id="dl-pdf" href="/download/pdf" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
    <div class="er">
      <span class="eico">&#128200;</span>
      <span class="enm" id="en-sap">auxiliar_sap_cruzado_YYYYMM.xlsx</span>
      <span class="est" id="es-sap">Pendiente</span>
      <a class="btn sec" id="dl-sap" href="/download/sap" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
    <div class="er">
      <span class="eico">&#128221;</span>
      <span class="enm" id="en-word">escrito_devolucion_IVA_YYYYMM.docx</span>
      <span class="est" id="es-word">Pendiente</span>
      <a class="btn sec" id="dl-word" href="/download/word" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
  </div>
  <div class="btn-row">
    <button class="btn danger" onclick="limpiar()">&#128260; Nuevo periodo</button>
  </div>
</div>

</div><!-- .content -->

<script>
// ─── SID cookie ───────────────────────────────────────────────────────────
function getSid(){
  let s = document.cookie.split(';').find(c=>c.trim().startsWith('sid='));
  if(s) return s.split('=')[1].trim();
  const n = crypto.randomUUID();
  document.cookie = 'sid='+n+';max-age='+7*86400+';samesite=lax';
  return n;
}
const SID = getSid();

// ─── Tabs ─────────────────────────────────────────────────────────────────
function showTab(n){
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById('panel-'+n).classList.add('active');
  document.getElementById('tab-'+n).classList.add('active');
}

// ─── Upload helpers ───────────────────────────────────────────────────────
function drag(e,z){e.preventDefault();document.getElementById('z-'+z).classList.add('drag')}
function undrag(z){document.getElementById('z-'+z).classList.remove('drag')}

async function drop(e,z){
  e.preventDefault(); undrag(z);
  const files = [...e.dataTransfer.files];
  await subirArchivos(files, z);
}
async function subirMultiple(inp, z){
  await subirArchivos([...inp.files], z);
  inp.value='';
}
async function subirUno(inp, z){
  if(inp.files.length) await subirArchivos([inp.files[0]], z);
  inp.value='';
}

async function subirArchivos(files, tipo){
  let ok=0;
  for(const f of files){
    const fd = new FormData();
    fd.append('file', f);
    fd.append('tipo', tipo);
    const r = await fetch('/upload', {method:'POST', body:fd,
                           headers:{'X-Sid':SID}});
    const d = await r.json();
    if(d.ok) ok++;
  }
  await actualizarEstado();
}

// ─── Guardar config ───────────────────────────────────────────────────────
async function guardarConfig(){
  const cfg = {
    empresa:    document.getElementById('c-empresa').value,
    rfc:        document.getElementById('c-rfc').value,
    domicilio:  document.getElementById('c-domicilio').value,
    clabe:      document.getElementById('c-clabe').value,
    rep_legal:  document.getElementById('c-rep').value,
    rfc_rep:    document.getElementById('c-rfcrep').value,
    autorizados:document.getElementById('c-aut').value,
    folio_sat:  document.getElementById('c-folio').value,
  };
  await fetch('/config',{method:'POST',
    headers:{'Content-Type':'application/json','X-Sid':SID},
    body:JSON.stringify(cfg)});
  const s = document.getElementById('cfg-saved');
  s.classList.remove('hidden');
  setTimeout(()=>s.classList.add('hidden'), 3000);
}

// ─── Estado inicial ───────────────────────────────────────────────────────
async function actualizarEstado(){
  const d = await (await fetch('/estado',{headers:{'X-Sid':SID}})).json();
  setZone('cfdi',    d.cfdi,    d.cfdi+' XML');
  setZone('pdf',     d.pdf,     d.pdf+' PDF');
  setZone('sap',     d.sap,     d.sap+' Excel');
  setZone('machote', d.machote, d.machote ? 'Cargado' : 'Opcional', true);
}
function setZone(z, n, txt, opcional){
  const el = document.getElementById('z-'+z);
  const st = document.getElementById('st-'+z);
  if(n>0 || n===true){el.classList.add('ok');st.textContent=txt;}
  else{el.classList.remove('ok');st.textContent=opcional?'Opcional':'Sin archivos';}
}

// ─── Progreso ─────────────────────────────────────────────────────────────
function upd(paso,pct,msg){
  const d=document.getElementById('d-'+paso);
  const b=document.getElementById('b-'+paso);
  const c=document.getElementById('c-'+paso);
  if(!d)return;
  const p=parseInt(pct);
  b.style.width=p+'%';
  if(p>=100){d.className='pdot ok';b.className='pb ok';}
  else{d.className='pdot run';b.className='pb';}
  if(c)c.textContent=msg.length>26?msg.substring(0,26)+'...':msg;
  log(msg, p>=100?'ok':'');
}
function log(msg,cls){
  const box=document.getElementById('logbox');
  const el=document.createElement('div');
  const t=new Date().toLocaleTimeString('es-MX',{hour12:false});
  el.textContent='['+t+'] '+msg;
  if(cls)el.className=cls;
  box.appendChild(el);
  box.scrollTop=box.scrollHeight;
}

// ─── Iniciar procesamiento ────────────────────────────────────────────────
let sse = null;
function iniciar(){
  const btn=document.getElementById('btn-proc');
  if(btn.disabled) return;
  btn.disabled=true;
  btn.textContent='... Procesando';
  document.getElementById('logbox').innerHTML='';
  ['cfdi','estado_cuenta','cruce_banco','auxiliar_sap',
   'excel','pdf','auxiliar_cruzado','word'].forEach(p=>{
    const d=document.getElementById('d-'+p);
    const b=document.getElementById('b-'+p);
    const c=document.getElementById('c-'+p);
    if(d)d.className='pdot';
    if(b){b.className='pb';b.style.width='0%';}
    if(c)c.textContent='—';
  });
  showTab('procesando');

  // Lanzar proceso y abrir SSE solo cuando el servidor confirme inicio
  fetch('/procesar',{method:'POST',headers:{'X-Sid':SID}})
    .then(r=>r.json())
    .then(d=>{
      if(d.error){ log(d.error,'err'); btn.disabled=false; btn.innerHTML='&#x25BA; PROCESAR AHORA'; return; }
      // Solo abrir SSE una vez confirmado el inicio
      if(sse){sse.close();}
      sse = new EventSource('/progreso?sid='+SID);
      sse.onmessage = function(e){
        const linea = e.data;
        if(linea.startsWith('PROGRESO:')){
          const p=linea.split(':'); if(p.length>=4) upd(p[1],p[2],p[3]);
        } else if(linea.startsWith('RESULTADO:')){
          const p=linea.split(':');
          if(p.length>=5) finProceso({total:p[1],iva:p[2],cruces:p[3],sin_cruce:p[4],
            trasladado:p[5]||'0',acreditable:p[6]||'0',saldo:p[7]||'0'});
        } else if(linea.startsWith('ERROR:')){
          log(linea.substring(6),'err');
          sse.close(); sse=null;
          document.getElementById('btn-proc').disabled=false;
          document.getElementById('btn-proc').innerHTML='&#x25BA; PROCESAR AHORA';
        } else if(linea.startsWith('DONE')){
          sse.close(); sse=null;
        } else if(linea.trim()){
          log(linea,'');
        }
      };
      sse.onerror = function(){
        if(sse) sse.close(); sse=null;
      };
    })
    .catch(e=>{ log('Error al iniciar: '+e,'err'); btn.disabled=false; btn.innerHTML='&#x25BA; PROCESAR AHORA'; });
}

function finProceso(d){
  if(sse){sse.close();sse=null;}
  document.getElementById('btn-proc').disabled=false;
  document.getElementById('btn-proc').innerHTML='&#x25BA; PROCESAR AHORA';
  const total=parseInt(d.total)||0, iva=parseFloat(d.iva)||0;
  const ok=parseInt(d.cruces)||0, sinc=parseInt(d.sin_cruce)||0;
  const parc=Math.max(0,total-ok-sinc);
  document.getElementById('m-iva').textContent='$'+iva.toLocaleString('es-MX',{minimumFractionDigits:2});
  const trasl=parseFloat(d.trasladado)||0, acred=parseFloat(d.acreditable)||0, saldo=parseFloat(d.saldo)||0;
  document.getElementById('m-trasl').textContent='$'+trasl.toLocaleString('es-MX',{minimumFractionDigits:2});
  document.getElementById('m-acred').textContent='$'+acred.toLocaleString('es-MX',{minimumFractionDigits:2});
  document.getElementById('m-saldo').textContent='$'+saldo.toLocaleString('es-MX',{minimumFractionDigits:2});
  document.getElementById('m-cru').textContent=ok+' / '+total+(total>0?' ('+Math.round(ok/total*100)+'%)':'');
  document.getElementById('m-par').textContent=parc;
  document.getElementById('m-sin').textContent=sinc;
  if(sinc>0) document.getElementById('al-sin').classList.remove('hidden');
  document.getElementById('resumen').innerHTML=
    '<b>CFDIs:</b> '+total+' &nbsp;|&nbsp; '+
    '<b>Saldo a favor a solicitar:</b> $'+iva.toLocaleString('es-MX',{minimumFractionDigits:2})+
    ' &nbsp;|&nbsp; <b>Cruce completo:</b> '+ok+
    ' &nbsp;|&nbsp; <b>Parcial:</b> '+parc+
    ' &nbsp;|&nbsp; <b>Sin cruce:</b> '+sinc;
  // Actualizar entregables
  fetch('/archivos_output',{headers:{'X-Sid':SID}}).then(r=>r.json()).then(od=>{
    ['excel','pdf','sap','word'].forEach(k=>{
      if(od[k]){
        document.getElementById('en-'+k).textContent=od[k];
        const st=document.getElementById('es-'+k);
        st.textContent='Listo ✓'; st.className='est ok';
      }
    });
  });
  showTab('entregables');
}

async function limpiar(){
  if(!confirm('Limpiar archivos de output y comenzar nuevo periodo?')) return;
  await fetch('/limpiar',{method:'POST',headers:{'X-Sid':SID}});
  ['excel','pdf','sap','word'].forEach(k=>{
    document.getElementById('es-'+k).textContent='Pendiente';
    document.getElementById('es-'+k).className='est';
  });
  showTab('archivos');
}

actualizarEstado();
</script>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

def _sid_from_request() -> str:
    """Lee X-Sid header o cookie."""
    sid = request.headers.get("X-Sid") or request.args.get("sid") or \
          request.cookies.get("sid", "")
    if not sid or len(sid) < 10:
        sid = str(uuid.uuid4())
    return sid


def _check_sid(sid: str) -> bool:
    """Valida que el SID solo contenga caracteres de UUID."""
    import re
    return bool(re.match(r'^[0-9a-f\-]{36}$', sid))


ALLOWED_EXT = {
    "cfdi":        {".xml"},
    "cfdi_cobro":  {".xml"},
    "pdf":         {".pdf"},
    "sap":         {".xlsx", ".xls"},
    "machote":     {".docx"},
}

OUTPUT_MAP = {
    "excel": ("reporte_iva_",     ".xlsx"),
    "pdf":   ("estado_cuenta_",   ".pdf"),
    "sap":   ("auxiliar_sap_",    ".xlsx"),
    "word":  ("escrito_devolucion_", ".docx"),
}


# ══════════════════════════════════════════════════════════════════════════
# Rutas
# ══════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    sid = _sid_from_request()
    _session_dir(sid)       # asegurar estructura de carpetas
    resp = make_response(HTML)
    resp.set_cookie("sid", sid, max_age=7 * 24 * 3600, samesite="Lax")
    return resp


@app.route("/upload", methods=["POST"])
def upload():
    sid  = _sid_from_request()
    if not _check_sid(sid):
        return jsonify({"ok": False, "error": "sid invalido"}), 400
    tipo = request.form.get("tipo", "")
    f    = request.files.get("file")
    if not f or tipo not in ALLOWED_EXT:
        return jsonify({"ok": False, "error": "tipo o archivo invalido"}), 400

    ext = Path(f.filename).suffix.lower()
    if ext not in ALLOWED_EXT[tipo]:
        return jsonify({"ok": False, "error": f"extension {ext} no permitida para {tipo}"}), 400

    base = _session_dir(sid)
    destdir_map = {
        "cfdi":       base / "input" / "cfdi",
        "cfdi_cobro": base / "input" / "cfdi_cobro",
        "pdf":        base / "input" / "estado_cuenta",
        "sap":        base / "input" / "auxiliar",
        "machote":    base / "input" / "machote",
    }
    nombre = secure_filename(f.filename)
    dest   = destdir_map[tipo] / nombre
    f.save(str(dest))
    return jsonify({"ok": True, "nombre": nombre})


@app.route("/config", methods=["POST"])
def guardar_config():
    sid = _sid_from_request()
    if not _check_sid(sid):
        return jsonify({"ok": False}), 400
    base = _session_dir(sid)
    datos = request.get_json(force=True, silent=True) or {}
    with open(base / "input" / "config.json", "w", encoding="utf-8") as fh:
        json.dump(datos, fh, ensure_ascii=False, indent=2)
    return jsonify({"ok": True})


@app.route("/estado")
def estado():
    sid = _sid_from_request()
    if not _check_sid(sid):
        return jsonify({"cfdi": 0, "pdf": 0, "sap": 0, "machote": False})
    base = _session_dir(sid)
    cfdi_dir       = base / "input" / "cfdi"
    cfdi_cobro_dir = base / "input" / "cfdi_cobro"
    pdf_dir        = base / "input" / "estado_cuenta"
    sap_dir        = base / "input" / "auxiliar"
    doc_dir        = base / "input" / "machote"
    return jsonify({
        "cfdi":       len(list(cfdi_dir.glob("*.xml")) + list(cfdi_dir.glob("*.XML"))),
        "cfdi_cobro": len(list(cfdi_cobro_dir.glob("*.xml")) + list(cfdi_cobro_dir.glob("*.XML"))),
        "pdf":        len(list(pdf_dir.glob("*.pdf"))  + list(pdf_dir.glob("*.PDF"))),
        "sap":        len(list(sap_dir.glob("*.xlsx")) + list(sap_dir.glob("*.xls")) +
                          list(sap_dir.glob("*.XLSX"))),
        "machote":    bool(list(doc_dir.glob("*.docx"))),
    })


@app.route("/procesar", methods=["POST"])
def procesar():
    """Lanza agente en background; el progreso se lee con /progreso (SSE)."""
    sid = request.headers.get("X-Sid") or request.cookies.get("sid", "")
    if not _check_sid(sid):
        return jsonify({"ok": False}), 400

    with _procs_lock:
        if sid in _procs and _procs[sid].poll() is None:
            return jsonify({"ok": False, "msg": "ya procesando"})

    base = _session_dir(sid)
    cmd  = [sys.executable, str(AGENTE_PY), str(base)]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    with _procs_lock:
        _procs[sid] = proc
    return jsonify({"ok": True})


@app.route("/progreso")
def progreso():
    """SSE stream: envía líneas de stdout del subprocess."""
    sid = request.args.get("sid", "")
    if not _check_sid(sid):
        return Response("", status=400)

    def _generar():
        # Esperar hasta que haya proceso (máx 5 s)
        for _ in range(50):
            with _procs_lock:
                proc = _procs.get(sid)
            if proc:
                break
            time.sleep(0.1)
        else:
            yield "data: ERROR:No se encontro proceso activo\n\n"
            return

        # Leer stdout línea a línea
        for linea in proc.stdout:
            linea = linea.rstrip()
            if linea:
                yield f"data: {linea}\n\n"

        proc.wait()
        yield "data: DONE\n\n"

    return Response(
        stream_with_context(_generar()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # desactivar buffer en Nginx/Render
        },
    )


@app.route("/archivos_output")
def archivos_output():
    sid = _sid_from_request()
    if not _check_sid(sid):
        return jsonify({})
    base = _session_dir(sid) / "output"
    res  = {}
    for key, (pre, ext) in OUTPUT_MAP.items():
        for f in base.glob(f"{pre}*{ext}"):
            res[key] = f.name
            break
    return jsonify(res)


@app.route("/download/<tipo>")
def download(tipo: str):
    sid = _sid_from_request()
    if not _check_sid(sid) or tipo not in OUTPUT_MAP:
        return "No encontrado", 404
    pre, ext = OUTPUT_MAP[tipo]
    base = _session_dir(sid) / "output"
    for f in base.glob(f"{pre}*{ext}"):
        return send_file(str(f), as_attachment=True, download_name=f.name)
    return "Archivo no generado aun", 404


@app.route("/limpiar", methods=["POST"])
def limpiar():
    sid = _sid_from_request()
    if not _check_sid(sid):
        return jsonify({"ok": False}), 400
    out = _session_dir(sid) / "output"
    for item in out.iterdir():
        try:
            item.unlink() if item.is_file() else shutil.rmtree(item)
        except Exception:
            pass
    return jsonify({"ok": True})


# ══════════════════════════════════════════════════════════════════════════
# Main (desarrollo local)
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import webbrowser, threading as _t
    port = int(os.environ.get("PORT", 5000))
    _t.Timer(1.0, lambda: webbrowser.open(f"http://localhost:{port}")).start()
    app.run(host="0.0.0.0", port=port, debug=False)
