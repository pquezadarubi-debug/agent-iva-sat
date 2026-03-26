# -*- coding: utf-8 -*-
"""
app.py — AgentIVA Devoluciones SAT — versión web (Flask)
Sistema con login: cada usuario tiene su propio espacio de trabajo persistente.
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
import functools
import re
from pathlib import Path

from flask import (Flask, request, Response, send_file,
                   jsonify, make_response, stream_with_context,
                   session, redirect)
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "agentiva-secret-2026-cambiar-en-prod")

# ─── Directorio de sesiones ────────────────────────────────────────────────
# En Render /tmp es efímero pero suficiente para procesamiento
SESSIONS_DIR = Path(os.environ.get("SESSIONS_DIR", "/tmp/agentiva"))
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# Ruta al motor principal (mismo directorio que app.py)
AGENTE_PY = Path(__file__).parent / "agente_iva.py"

# ─── Estado de procesos activos {sid: subprocess} ─────────────────────────
_procs: dict[str, subprocess.Popen] = {}
_procs_lock = threading.Lock()

# ─── SAT download threads {sid: thread} ────────────────────────────────────
_sat_threads: dict[str, threading.Thread] = {}
_sat_threads_lock = threading.Lock()

# ─── Archivo de usuarios ───────────────────────────────────────────────────
USERS_FILE = SESSIONS_DIR / "users.json"


def _load_users() -> dict:
    if USERS_FILE.exists():
        try:
            return json.loads(USERS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_users(users: dict):
    USERS_FILE.write_text(json.dumps(users, ensure_ascii=False, indent=2),
                          encoding="utf-8")


def requires_login(f):
    """Decorador: redirige a /login si no hay sesión activa."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("username"):
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated


# ══════════════════════════════════════════════════════════════════════════
# Helpers de sesión
# ══════════════════════════════════════════════════════════════════════════

def _session_dir(sid: str) -> Path:
    d = SESSIONS_DIR / sid
    for sub in ["input/cfdi_cobro", "input/cfdi_pago", "input/aux_cobrado",
                "input/aux_pagado", "input/pdf_bancos", "input/aux_bancos",
                "input/machote", "input/cfdi_facturas", "output",
                "input/cfdi", "input/estado_cuenta", "input/auxiliar"]:
        (d / sub).mkdir(parents=True, exist_ok=True)
    return d


def _get_sid() -> str:
    """Devuelve el SID: para usuarios logueados = 'u_username', anónimo = UUID cookie."""
    username = session.get("username")
    if username:
        return f"u_{username}"
    sid = request.headers.get("X-Sid") or request.args.get("sid") or \
          request.cookies.get("sid", "")
    if not sid or len(sid) < 10:
        sid = str(uuid.uuid4())
    return sid


def _limpiar_sesiones_antiguas():
    """Borra sesiones anónimas con más de 2 horas. Usuarios: 30 días."""
    while True:
        time.sleep(1800)
        ahora = time.time()
        if not SESSIONS_DIR.exists():
            continue
        for d in SESSIONS_DIR.iterdir():
            try:
                if not d.is_dir():
                    continue
                nombre = d.name
                if nombre.startswith("u_"):
                    # Sesión de usuario: limpiar tras 30 días sin uso
                    if (ahora - d.stat().st_mtime) > 30 * 86400:
                        shutil.rmtree(d, ignore_errors=True)
                elif len(nombre) == 36:
                    # Sesión anónima: limpiar tras 2 horas
                    if (ahora - d.stat().st_mtime) > 7200:
                        shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass


threading.Thread(target=_limpiar_sesiones_antiguas, daemon=True).start()


# ══════════════════════════════════════════════════════════════════════════
# Página de Login / Registro
# ══════════════════════════════════════════════════════════════════════════

LOGIN_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Agente IVA &mdash; Acceso</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',Arial,sans-serif;background:#1F4E79;
     display:flex;align-items:center;justify-content:center;min-height:100vh}
.card{background:#fff;border-radius:10px;padding:36px 40px;width:360px;
      box-shadow:0 8px 32px rgba(0,0,0,.3)}
.logo{font-size:28px;text-align:center;margin-bottom:4px}
.titulo{font-size:18px;font-weight:700;color:#1F4E79;text-align:center;margin-bottom:4px}
.sub{font-size:12px;color:#888;text-align:center;margin-bottom:28px}
.tabs{display:flex;gap:0;margin-bottom:24px;border-bottom:2px solid #e0e0e0}
.ltab{flex:1;padding:8px;text-align:center;cursor:pointer;font-size:13px;
      font-weight:600;color:#888;border-bottom:3px solid transparent;margin-bottom:-2px}
.ltab.active{color:#1F4E79;border-bottom-color:#1F4E79}
.panel{display:none}.panel.active{display:block}
.fld{margin-bottom:14px}
.fld label{display:block;font-size:12px;font-weight:600;color:#555;margin-bottom:4px}
.fld input{width:100%;border:1px solid #ddd;border-radius:5px;padding:9px 12px;
           font-size:13px;font-family:inherit;outline:none}
.fld input:focus{border-color:#2E75B6}
.btn{width:100%;background:#1F4E79;color:#fff;border:none;border-radius:5px;
     padding:11px;font-size:14px;font-weight:700;cursor:pointer;margin-top:6px}
.btn:hover{background:#2E75B6}
.msg{font-size:12px;padding:8px 12px;border-radius:4px;margin-top:10px;display:none}
.msg.err{background:#FCE4D6;color:#C00000;display:block}
.msg.ok{background:#E2EFDA;color:#375623;display:block}
.nota{font-size:10px;color:#aaa;text-align:center;margin-top:16px;line-height:1.5}
</style>
</head>
<body>
<div class="card">
  <div class="logo">&#128202;</div>
  <div class="titulo">Agente IVA</div>
  <div class="sub">Devoluciones SAT &mdash; M&eacute;xico</div>
  <div class="tabs">
    <div class="ltab active" id="tab-login" onclick="showTab('login')">Iniciar sesi&oacute;n</div>
    <div class="ltab" id="tab-reg" onclick="showTab('reg')">Crear cuenta</div>
  </div>

  <!-- LOGIN -->
  <div class="panel active" id="panel-login">
    <form onsubmit="doLogin(event)">
      <div class="fld"><label>Usuario</label>
        <input id="l-user" autocomplete="username" placeholder="tu_usuario" required></div>
      <div class="fld"><label>Contrase&ntilde;a</label>
        <input id="l-pass" type="password" autocomplete="current-password" required></div>
      <button class="btn" type="submit">Entrar</button>
      <div class="msg" id="l-msg"></div>
    </form>
  </div>

  <!-- REGISTRO -->
  <div class="panel" id="panel-reg">
    <form onsubmit="doReg(event)">
      <div class="fld"><label>Usuario (sin espacios)</label>
        <input id="r-user" placeholder="mi_empresa" pattern="[a-zA-Z0-9_\\-]+" required></div>
      <div class="fld"><label>Contrase&ntilde;a</label>
        <input id="r-pass" type="password" minlength="6" required></div>
      <div class="fld"><label>Confirmar contrase&ntilde;a</label>
        <input id="r-pass2" type="password" required></div>
      <button class="btn" type="submit">Crear cuenta</button>
      <div class="msg" id="r-msg"></div>
    </form>
  </div>

  <div class="nota">Tus archivos se guardan en tu cuenta.<br>
    En el plan gratuito se pierden si el servidor reinicia.</div>
</div>
<script>
function showTab(t){
  ['login','reg'].forEach(x=>{
    document.getElementById('panel-'+x).classList.toggle('active',x===t);
    document.getElementById('tab-'+x).classList.toggle('active',x===t);
  });
}
async function doLogin(e){
  e.preventDefault();
  const r=await fetch('/login',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({username:document.getElementById('l-user').value,
                         password:document.getElementById('l-pass').value})});
  const d=await r.json();
  const m=document.getElementById('l-msg');
  if(d.ok){m.className='msg ok';m.textContent='Acceso correcto, redirigiendo...';
            setTimeout(()=>location.href='/',800);}
  else{m.className='msg err';m.textContent=d.error||'Error al iniciar sesion';}
}
async function doReg(e){
  e.preventDefault();
  const p=document.getElementById('r-pass').value;
  const p2=document.getElementById('r-pass2').value;
  const m=document.getElementById('r-msg');
  if(p!==p2){m.className='msg err';m.textContent='Las contrasenas no coinciden';return;}
  const r=await fetch('/register',{method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({username:document.getElementById('r-user').value,password:p})});
  const d=await r.json();
  if(d.ok){m.className='msg ok';m.textContent='Cuenta creada, entrando...';
            setTimeout(()=>location.href='/',800);}
  else{m.className='msg err';m.textContent=d.error||'Error al crear cuenta';}
}
</script>
</body>
</html>
"""


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
.ubtn-del{position:absolute;top:6px;right:6px;background:#C00000;color:#fff;
          border:none;border-radius:3px;padding:2px 7px;font-size:10px;
          cursor:pointer;z-index:10;display:none}
.uzone.ok .ubtn-del{display:block}
.ubtn-del:hover{background:#a00000}
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
/* SAT download */
.sat-fiel-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}
.sat-fiel-zone{border:2px dashed var(--borde);border-radius:6px;padding:14px;
  text-align:center;cursor:pointer;transition:border-color .15s}
.sat-fiel-zone.ok{border-color:#4CAF50;background:var(--vbg)}
.sat-fiel-zone label{cursor:pointer;display:block}
.sat-fiel-zone input[type=file]{display:none}
.sat-log{background:#1e1e2e;color:#cdd6f4;font-family:monospace;font-size:11px;
  padding:10px 12px;height:180px;overflow-y:auto;border-radius:4px;margin-top:12px;line-height:1.6}
.sat-log .ok{color:#a6e3a1}.sat-log .warn{color:#f9e2af}.sat-log .err{color:#f38ba8}
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
  <div class="badge-pub" style="display:flex;align-items:center;gap:12px">
    {{USER_BADGE}}
  </div>
</header>
<div class="tabs">
  <div class="tab active" onclick="showTab('archivos')"    id="tab-archivos">&#128193; Archivos</div>
  <div class="tab"        onclick="showTab('sat')"         id="tab-sat">&#128275; Descarga SAT</div>
  <div class="tab"        onclick="showTab('procesando')"  id="tab-procesando">&#9881;&#65039; Procesando</div>
  <div class="tab"        onclick="showTab('resultados')"  id="tab-resultados">&#128200; Resultados</div>
  <div class="tab"        onclick="showTab('entregables')" id="tab-entregables">&#128230; Entregables</div>
</div>
<div class="content">

<!-- TAB 1 ARCHIVOS -->
<div class="panel active" id="panel-archivos">

  <!-- SECCION 1: IVA COBRADO -->
  <div class="card">
    <div class="ctitle" style="color:#1F4E79">&#128200; IVA Cobrado &mdash; Trasladado</div>
    <div class="upload-grid">
      <div class="uzone" id="z-cfdi-cobro" ondragover="drag(event,'cfdi-cobro')"
           ondragleave="undrag('cfdi-cobro')" ondrop="drop(event,'cfdi-cobro')">
        <button class="ubtn-del" onclick="borrarZona(event,'cfdi-cobro')">&#10005; Borrar</button>
        <input type="file" multiple accept=".xml,.XML"
               onchange="subirMultiple(this,'cfdi-cobro')" id="inp-cfdi-cobro">
        <div class="uicon">&#128196;</div>
        <div class="ulabel">CFDIs de Cobro</div>
        <div class="usub">Emisor = tu empresa &mdash; .xml (varios)</div>
        <div class="ust" id="st-cfdi-cobro">Sin archivos</div>
      </div>
      <div class="uzone" id="z-aux-cobrado" ondragover="drag(event,'aux-cobrado')"
           ondragleave="undrag('aux-cobrado')" ondrop="drop(event,'aux-cobrado')">
        <button class="ubtn-del" onclick="borrarZona(event,'aux-cobrado')">&#10005; Borrar</button>
        <input type="file" multiple accept=".xlsx,.xls,.XLSX,.XLS"
               onchange="subirMultiple(this,'aux-cobrado')" id="inp-aux-cobrado">
        <div class="uicon">&#128202;</div>
        <div class="ulabel">Auxiliar SAP IVA Cobrado</div>
        <div class="usub">Auxiliar contable IVA trasladado &mdash; .xlsx</div>
        <div class="ust" id="st-aux-cobrado">Sin archivos</div>
      </div>
    </div>
  </div>

  <!-- SECCION 2: IVA PAGADO -->
  <div class="card">
    <div class="ctitle" style="color:#7F6000">&#128199; IVA Pagado &mdash; Acreditable</div>
    <div class="upload-grid">
      <div class="uzone" id="z-cfdi-pago" ondragover="drag(event,'cfdi-pago')"
           ondragleave="undrag('cfdi-pago')" ondrop="drop(event,'cfdi-pago')">
        <button class="ubtn-del" onclick="borrarZona(event,'cfdi-pago')">&#10005; Borrar</button>
        <input type="file" multiple accept=".xml,.XML"
               onchange="subirMultiple(this,'cfdi-pago')" id="inp-cfdi-pago">
        <div class="uicon">&#128196;</div>
        <div class="ulabel">CFDIs de Pago</div>
        <div class="usub">Emisor = proveedor &mdash; .xml (varios)</div>
        <div class="ust" id="st-cfdi-pago">Sin archivos</div>
      </div>
      <div class="uzone" id="z-aux-pagado" ondragover="drag(event,'aux-pagado')"
           ondragleave="undrag('aux-pagado')" ondrop="drop(event,'aux-pagado')">
        <button class="ubtn-del" onclick="borrarZona(event,'aux-pagado')">&#10005; Borrar</button>
        <input type="file" multiple accept=".xlsx,.xls,.XLSX,.XLS"
               onchange="subirMultiple(this,'aux-pagado')" id="inp-aux-pagado">
        <div class="uicon">&#128202;</div>
        <div class="ulabel">Auxiliar SAP IVA Pagado</div>
        <div class="usub">Auxiliar contable IVA acreditable &mdash; .xlsx</div>
        <div class="ust" id="st-aux-pagado">Sin archivos</div>
      </div>
    </div>
  </div>

  <!-- SECCION 3: BANCOS -->
  <div class="card">
    <div class="ctitle" style="color:#375623">&#127981; Bancos</div>
    <div class="upload-grid">
      <div class="uzone" id="z-pdf-bancos" ondragover="drag(event,'pdf-bancos')"
           ondragleave="undrag('pdf-bancos')" ondrop="drop(event,'pdf-bancos')">
        <button class="ubtn-del" onclick="borrarZona(event,'pdf-bancos')">&#10005; Borrar</button>
        <input type="file" multiple accept=".pdf,.PDF"
               onchange="subirMultiple(this,'pdf-bancos')" id="inp-pdf-bancos">
        <div class="uicon">&#128203;</div>
        <div class="ulabel">Estados de Cuenta Bancarios</div>
        <div class="usub">PDF bancarios &mdash; .pdf (varios)</div>
        <div class="ust" id="st-pdf-bancos">Sin archivos</div>
      </div>
      <div class="uzone" id="z-aux-bancos" ondragover="drag(event,'aux-bancos')"
           ondragleave="undrag('aux-bancos')" ondrop="drop(event,'aux-bancos')">
        <button class="ubtn-del" onclick="borrarZona(event,'aux-bancos')">&#10005; Borrar</button>
        <input type="file" multiple accept=".xlsx,.xls,.XLSX,.XLS"
               onchange="subirMultiple(this,'aux-bancos')" id="inp-aux-bancos">
        <div class="uicon">&#128202;</div>
        <div class="ulabel">Auxiliar Contable Bancos</div>
        <div class="usub">Cargos (cobros) y abonos (pagos) &mdash; .xlsx</div>
        <div class="ust" id="st-aux-bancos">Sin archivos</div>
      </div>
    </div>
  </div>

  <!-- SECCION OPCIONAL: Facturas tipo I (si se tienen manualmente) -->
  <div class="card">
    <div class="ctitle" style="color:#5C3D2E">&#128203; Facturas relacionadas — Opcional</div>
    <p style="font-size:11px;color:#666;margin-bottom:10px">
      Si tienes los XML de las facturas (tipo I, Ingreso) relacionadas a los pagos, s&uacute;belas aqu&iacute;.
      Si usas la <strong>Descarga SAT</strong>, se descargan autom&aacute;ticamente.
      Permiten enriquecer los reportes con los <em>conceptos</em> de cada factura para el an&aacute;lisis de riesgos.
    </p>
    <div style="max-width:340px">
      <div class="uzone" id="z-cfdi-facturas" ondragover="drag(event,'cfdi-facturas')"
           ondragleave="undrag('cfdi-facturas')" ondrop="drop(event,'cfdi-facturas')">
        <button class="ubtn-del" onclick="borrarZona(event,'cfdi-facturas')">&#10005; Borrar</button>
        <input type="file" multiple accept=".xml,.XML"
               onchange="subirMultiple(this,'cfdi-facturas')" id="inp-cfdi-facturas">
        <div class="uicon">&#128196;</div>
        <div class="ulabel">Facturas tipo I relacionadas</div>
        <div class="usub">XML de facturas pagadas o cobradas</div>
        <div class="ust" id="st-cfdi-facturas">Opcional</div>
      </div>
    </div>
  </div>

  <!-- Machote eliminado: el escrito se genera automáticamente desde plantilla integrada -->

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
    <div class="pit"><div class="pdot" id="d-riesgos"></div><div class="plbl">An&aacute;lisis de Riesgos IA</div><div class="pbw"><div class="pb" id="b-riesgos"></div></div><div class="pcnt" id="c-riesgos">—</div></div>
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
      <span class="enm" id="en-excel">reporte_IVA_Acreditable_YYYYMM.xlsx</span>
      <span class="est" id="es-excel">Pendiente</span>
      <a class="btn sec" id="dl-excel" href="/download/excel" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
    <div class="er">
      <span class="eico">&#128202;</span>
      <span class="enm" id="en-excel-cobro">reporte_IVA_Trasladado_YYYYMM.xlsx</span>
      <span class="est" id="es-excel-cobro">Pendiente</span>
      <a class="btn sec" id="dl-excel-cobro" href="/download/excel_cobro" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
    <div class="er">
      <span class="eico">&#128203;</span>
      <span class="enm" id="en-pdf">estado_cuenta_cruzado_YYYYMM.pdf</span>
      <span class="est" id="es-pdf">Pendiente</span>
      <a class="btn sec" id="dl-pdf" href="/download/pdf" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
    <div class="er">
      <span class="eico">&#128200;</span>
      <span class="enm" id="en-sap">auxiliar_IVA_Acreditable_YYYYMM.xlsx</span>
      <span class="est" id="es-sap">Pendiente</span>
      <a class="btn sec" id="dl-sap" href="/download/sap" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
    <div class="er">
      <span class="eico">&#128200;</span>
      <span class="enm" id="en-sap-cobro">auxiliar_IVA_Trasladado_YYYYMM.xlsx</span>
      <span class="est" id="es-sap-cobro">Pendiente</span>
      <a class="btn sec" id="dl-sap-cobro" href="/download/sap_cobro" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
    <div class="er">
      <span class="eico">&#128221;</span>
      <span class="enm" id="en-word">escrito_devolucion_IVA_YYYYMM.docx</span>
      <span class="est" id="es-word">Pendiente</span>
      <a class="btn sec" id="dl-word" href="/download/word" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
    <div class="er">
      <span class="eico">&#128680;</span>
      <span class="enm" id="en-riesgos">reporte_riesgos_YYYYMM.xlsx <span style="font-size:10px;color:#888">(requiere ANTHROPIC_API_KEY o GEMINI_API_KEY)</span></span>
      <span class="est" id="es-riesgos">Pendiente</span>
      <a class="btn sec" id="dl-riesgos" href="/download/riesgos" style="padding:4px 12px;font-size:11px;text-decoration:none">&#8659; Descargar</a>
    </div>
  </div>
  <div class="btn-row">
    <button class="btn danger" onclick="limpiar()">&#128260; Nuevo periodo</button>
  </div>
</div>

<!-- TAB SAT: Descarga automática del SAT -->
<div class="panel" id="panel-sat">

  <div class="card">
    <div class="ctitle" style="color:#C00000">&#128275; Descarga automática de CFDIs del SAT</div>
    <p style="font-size:12px;color:#555;margin-bottom:12px">
      Descarga tus CFDIs directamente del SAT usando tu <strong>e.firma (FIEL)</strong>.
      Solo necesitas el archivo <code>.cer</code>, el <code>.key</code> y la contraseña.
    </p>

    <!-- Certificados FIEL -->
    <div class="sat-fiel-grid">
      <div class="sat-fiel-zone" id="z-sat-cer" onclick="document.getElementById('inp-sat-cer').click()">
        <input type="file" id="inp-sat-cer" accept=".cer,.CER" onchange="seleccionarFIEL(this,'cer')">
        <label for="inp-sat-cer">
          <div style="font-size:28px">&#128220;</div>
          <div style="font-weight:600;margin:4px 0">Certificado .cer</div>
          <div id="st-sat-cer" style="font-size:11px;color:#888">Sin archivo</div>
        </label>
      </div>
      <div class="sat-fiel-zone" id="z-sat-key" onclick="document.getElementById('inp-sat-key').click()">
        <input type="file" id="inp-sat-key" accept=".key,.KEY" onchange="seleccionarFIEL(this,'key')">
        <label for="inp-sat-key">
          <div style="font-size:28px">&#128274;</div>
          <div style="font-weight:600;margin:4px 0">Llave privada .key</div>
          <div id="st-sat-key" style="font-size:11px;color:#888">Sin archivo</div>
        </label>
      </div>
    </div>

    <!-- Contrasena -->
    <div class="cfg-form" style="grid-template-columns:1fr 1fr 1fr;margin-bottom:10px">
      <div class="fld">
        <label>Contrase&ntilde;a e.firma</label>
        <input type="password" id="sat-pass" placeholder="Contrase&ntilde;a del certificado">
      </div>
      <div class="fld">
        <label>Fecha inicio</label>
        <input type="date" id="sat-fecha-ini">
      </div>
      <div class="fld">
        <label>Fecha fin</label>
        <input type="date" id="sat-fecha-fin">
      </div>
    </div>

    <!-- Tipo de CFDIs a descargar -->
    <div style="display:flex;gap:24px;margin-bottom:14px;font-size:13px">
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer">
        <input type="checkbox" id="sat-emitidos" checked>
        <strong>Emitidos</strong> &mdash; IVA Cobrado (empresa = emisor)
      </label>
      <label style="display:flex;align-items:center;gap:6px;cursor:pointer">
        <input type="checkbox" id="sat-recibidos" checked>
        <strong>Recibidos</strong> &mdash; IVA Pagado (empresa = receptor)
      </label>
    </div>

    <div class="btn-row">
      <button class="btn" id="btn-sat" onclick="descargarSAT()">&#9654; Descargar CFDIs del SAT</button>
    </div>
  </div>

  <!-- Progreso de descarga SAT -->
  <div class="card" id="sat-progress-card">
    <div class="ctitle">Progreso de descarga SAT</div>
    <div class="sat-log" id="sat-logbox"></div>
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
  setZone('cfdi-cobro',    d.cfdi_cobro,    d.cfdi_cobro+' XML');
  setZone('cfdi-pago',     d.cfdi_pago,     d.cfdi_pago+' XML');
  setZone('cfdi-facturas', d.cfdi_facturas, d.cfdi_facturas+' XML (facturas)', true);
  setZone('aux-cobrado',   d.aux_cobrado,   d.aux_cobrado+' Excel');
  setZone('aux-pagado',    d.aux_pagado,    d.aux_pagado+' Excel');
  setZone('pdf-bancos',    d.pdf_bancos,    d.pdf_bancos+' PDF');
  setZone('aux-bancos',    d.aux_bancos,    d.aux_bancos+' Excel');
  setZone('machote',       d.machote,       d.machote ? 'Cargado' : 'Opcional', true);
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
   'excel','pdf','auxiliar_cruzado','word','riesgos'].forEach(p=>{
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
      sse.onmessage = manejarSSE;
      sse.onerror = function(){if(sse)sse.close();sse=null;};
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
    ['excel','excel_cobro','pdf','sap','sap_cobro','word','riesgos'].forEach(k=>{
      const enEl=document.getElementById('en-'+k);
      const stEl=document.getElementById('es-'+k);
      if(od[k]&&enEl){enEl.textContent=od[k];}
      if(od[k]&&stEl){stEl.textContent='Listo \u2713';stEl.className='est ok';}
    });
  });
  showTab('entregables');
}

async function borrarZona(e, tipo){
  e.stopPropagation(); e.preventDefault();
  if(!confirm('Borrar todos los archivos de esta zona?')) return;
  await fetch('/limpiar_zona',{method:'POST',
    headers:{'Content-Type':'application/json','X-Sid':SID},
    body:JSON.stringify({tipo})});
  await actualizarEstado();
}

async function limpiar(){
  if(!confirm('Limpiar archivos de output y comenzar nuevo periodo?')) return;
  await fetch('/limpiar',{method:'POST',headers:{'X-Sid':SID}});
  ['excel','excel_cobro','pdf','sap','sap_cobro','word','riesgos'].forEach(k=>{
    const el=document.getElementById('es-'+k);
    if(el){el.textContent='Pendiente';el.className='est';}
  });
  showTab('archivos');
}

actualizarEstado();

// Al cargar: verificar si hay proceso activo o terminado
(async function verificarProceso(){
  const d = await (await fetch('/estado_proceso',{headers:{'X-Sid':SID}})).json();
  if(d.activo){
    // Proceso corriendo — reconectar SSE
    log('Proceso en curso detectado, reconectando...','warn');
    showTab('procesando');
    document.getElementById('btn-proc').disabled=true;
    document.getElementById('btn-proc').textContent='... Procesando';
    if(sse){sse.close();}
    sse = new EventSource('/progreso?sid='+SID);
    sse.onmessage = manejarSSE;
    sse.onerror = function(){if(sse)sse.close();sse=null;};
  } else if(d.terminado && d.log){
    // Proceso ya terminó — reproducir resultado del log
    const lineas = d.log.split('\n');
    for(const l of lineas){
      const linea = l.trim();
      if(!linea) continue;
      if(linea.startsWith('PROGRESO:')){
        const p=linea.split(':');if(p.length>=4)upd(p[1],p[2],p[3]);
      } else if(linea.startsWith('RESULTADO:')){
        const p=linea.split(':');
        if(p.length>=5) finProceso({total:p[1],iva:p[2],cruces:p[3],sin_cruce:p[4],
          trasladado:p[5]||'0',acreditable:p[6]||'0',saldo:p[7]||'0'});
      }
    }
  }
})();

function manejarSSE(e){
  const linea=e.data;
  if(linea.startsWith('PROGRESO:')){
    const p=linea.split(':');if(p.length>=4)upd(p[1],p[2],p[3]);
  } else if(linea.startsWith('RESULTADO:')){
    const p=linea.split(':');
    if(p.length>=5) finProceso({total:p[1],iva:p[2],cruces:p[3],sin_cruce:p[4],
      trasladado:p[5]||'0',acreditable:p[6]||'0',saldo:p[7]||'0'});
  } else if(linea.startsWith('ERROR:')){
    log(linea.substring(6),'err');
    if(sse){sse.close();sse=null;}
    document.getElementById('btn-proc').disabled=false;
    document.getElementById('btn-proc').innerHTML='&#x25BA; PROCESAR AHORA';
  } else if(linea.startsWith('DONE')){
    if(sse){sse.close();sse=null;}
  } else if(linea.trim()){log(linea,'');}
}

// ─── SAT FIEL selección ───────────────────────────────────────────────────
const satFiles = {cer: null, key: null};
function seleccionarFIEL(inp, tipo){
  const f = inp.files[0];
  if(!f) return;
  satFiles[tipo] = f;
  document.getElementById('st-sat-'+tipo).textContent = f.name;
  document.getElementById('z-sat-'+tipo).classList.add('ok');
}

// ─── SAT download ─────────────────────────────────────────────────────────
let sseSAT = null;
async function descargarSAT(){
  const btn = document.getElementById('btn-sat');
  const logbox = document.getElementById('sat-logbox');

  if(!satFiles.cer || !satFiles.key){
    satLogMsg('ERROR: Selecciona los archivos .cer y .key de tu e.firma','err');
    return;
  }
  const pass = document.getElementById('sat-pass').value;
  if(!pass){ satLogMsg('ERROR: Ingresa la contraseña de tu e.firma','err'); return; }
  const fechaIni = document.getElementById('sat-fecha-ini').value;
  const fechaFin = document.getElementById('sat-fecha-fin').value;
  if(!fechaIni || !fechaFin){ satLogMsg('ERROR: Selecciona el rango de fechas','err'); return; }
  const emitidos  = document.getElementById('sat-emitidos').checked;
  const recibidos = document.getElementById('sat-recibidos').checked;
  if(!emitidos && !recibidos){ satLogMsg('ERROR: Selecciona al menos un tipo (emitidos o recibidos)','err'); return; }

  btn.disabled = true;
  btn.textContent = '... Descargando';
  logbox.innerHTML = '';
  document.getElementById('sat-progress-card').style.display = '';

  const fd = new FormData();
  fd.append('cer_file', satFiles.cer);
  fd.append('key_file', satFiles.key);
  fd.append('password', pass);
  fd.append('fecha_ini', fechaIni);
  fd.append('fecha_fin', fechaFin);
  fd.append('emitidos',  emitidos  ? '1' : '0');
  fd.append('recibidos', recibidos ? '1' : '0');

  const r = await fetch('/descargar_sat', {method:'POST', body:fd, headers:{'X-Sid':SID}});
  const d = await r.json();
  if(!d.ok){ satLogMsg('ERROR: '+d.error,'err'); btn.disabled=false; btn.textContent='\u25BA Descargar CFDIs del SAT'; return; }

  // SSE para progreso
  if(sseSAT) sseSAT.close();
  sseSAT = new EventSource('/progreso_sat?sid='+SID);
  sseSAT.onmessage = function(e){
    const linea = e.data;
    if(linea.startsWith('RESULTADO_SAT:OK')){
      satLogMsg('Descarga completada. Ve a la pestaña Archivos para procesar.','ok');
      sseSAT.close(); sseSAT=null;
      btn.disabled=false; btn.innerHTML='&#9654; Descargar CFDIs del SAT';
      actualizarEstado();
    } else if(linea.startsWith('ERROR:')){
      satLogMsg(linea.substring(6),'err');
      sseSAT.close(); sseSAT=null;
      btn.disabled=false; btn.innerHTML='&#9654; Descargar CFDIs del SAT';
    } else if(linea === 'DONE'){
      sseSAT.close(); sseSAT=null;
      btn.disabled=false; btn.innerHTML='&#9654; Descargar CFDIs del SAT';
    } else if(linea.trim()){
      satLogMsg(linea, linea.includes('ERROR') ? 'err' : linea.includes('OK') ? 'ok' : '');
    }
  };
  sseSAT.onerror = function(){ if(sseSAT){sseSAT.close();sseSAT=null;} };
}

function satLogMsg(msg, cls){
  const box = document.getElementById('sat-logbox');
  const el = document.createElement('div');
  const t = new Date().toLocaleTimeString('es-MX',{hour12:false});
  el.textContent = '['+t+'] '+msg;
  if(cls) el.className = cls;
  box.appendChild(el);
  box.scrollTop = box.scrollHeight;
}
</script>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

def _sid_from_request() -> str:
    """SID: para usuarios logueados = u_username; anónimo = UUID header/cookie."""
    username = session.get("username")
    if username:
        return f"u_{username}"
    sid = request.headers.get("X-Sid") or request.args.get("sid") or \
          request.cookies.get("sid", "")
    if not sid or len(sid) < 10:
        sid = str(uuid.uuid4())
    return sid


def _check_sid(sid: str) -> bool:
    """Valida SID: UUID anónimo o u_username de usuario logueado."""
    return bool(re.match(r'^[0-9a-f\-]{36}$', sid)) or \
           bool(re.match(r'^u_[a-zA-Z0-9_\-]{1,50}$', sid))


ALLOWED_EXT = {
    "cfdi-cobro":     {".xml"},
    "cfdi-pago":      {".xml"},
    "cfdi-facturas":  {".xml"},
    "aux-cobrado":    {".xlsx", ".xls"},
    "aux-pagado":     {".xlsx", ".xls"},
    "pdf-bancos":     {".pdf"},
    "aux-bancos":     {".xlsx", ".xls"},
    "machote":        {".docx"},
}

OUTPUT_MAP = {
    "excel":       ("reporte_IVA_Acreditable_",  ".xlsx"),
    "excel_cobro": ("reporte_IVA_Trasladado_",   ".xlsx"),
    "pdf":         ("estado_cuenta_",             ".pdf"),
    "sap":         ("auxiliar_IVA_Acreditable_",  ".xlsx"),
    "sap_cobro":   ("auxiliar_IVA_Trasladado_",   ".xlsx"),
    "word":        ("escrito_devolucion_",         ".docx"),
    "riesgos":     ("reporte_riesgos_",           ".xlsx"),
}


# ══════════════════════════════════════════════════════════════════════════
# Rutas
# ══════════════════════════════════════════════════════════════════════════

@app.route("/login", methods=["GET"])
def login_page():
    if session.get("username"):
        return redirect("/")
    return make_response(LOGIN_HTML)


@app.route("/login_opcional")
def login_opcional():
    """Redirige a login pero con opción de volver."""
    return redirect("/login")


@app.route("/login", methods=["POST"])
def login_post():
    datos = request.get_json(force=True, silent=True) or {}
    username = datos.get("username", "").strip().lower()
    password = datos.get("password", "")
    if not username or not password:
        return jsonify({"ok": False, "error": "Usuario y contraseña requeridos"})

    # Verificar primero contra variables de entorno (siempre disponibles)
    admin_user = os.environ.get("ADMIN_USER", "").strip().lower()
    admin_pass = os.environ.get("ADMIN_PASS", "")
    if admin_user and username == admin_user and password == admin_pass:
        session["username"] = username
        session.permanent = True
        app.permanent_session_lifetime = datetime.timedelta(days=30)
        return jsonify({"ok": True})

    # Luego verificar usuarios registrados (en /tmp, pueden perderse al reiniciar)
    users = _load_users()
    if username not in users:
        return jsonify({"ok": False, "error": "Usuario no encontrado"})
    if not check_password_hash(users[username]["password_hash"], password):
        return jsonify({"ok": False, "error": "Contraseña incorrecta"})
    session["username"] = username
    session.permanent = True
    app.permanent_session_lifetime = datetime.timedelta(days=30)
    return jsonify({"ok": True})


@app.route("/register", methods=["POST"])
def register_post():
    datos = request.get_json(force=True, silent=True) or {}
    username = datos.get("username", "").strip().lower()
    password = datos.get("password", "")
    if not re.match(r'^[a-zA-Z0-9_\-]{2,50}$', username):
        return jsonify({"ok": False, "error": "Usuario inválido (solo letras, números, _ y -)"})
    if len(password) < 6:
        return jsonify({"ok": False, "error": "Contraseña mínimo 6 caracteres"})
    users = _load_users()
    if username in users:
        return jsonify({"ok": False, "error": "Ese usuario ya existe"})
    users[username] = {
        "password_hash": generate_password_hash(password),
        "created": datetime.datetime.now().isoformat()
    }
    _save_users(users)
    session["username"] = username
    session.permanent = True
    app.permanent_session_lifetime = datetime.timedelta(days=30)
    return jsonify({"ok": True})


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/")
def index():
    username = session.get("username", "")
    sid = _sid_from_request()
    _session_dir(sid)
    if username:
        badge = (f'<span>&#128100; {username}</span>'
                 f'<a href="/logout" style="color:rgba(255,255,255,.8);font-size:11px;text-decoration:none">'
                 f'Cerrar sesi&oacute;n</a>')
    else:
        badge = ('<span style="font-size:11px;opacity:.8">Sesion temporal &mdash; archivos se borran al cerrar</span>'
                 '<a href="/login" style="color:#fff;font-size:11px;font-weight:600;text-decoration:none;'
                 'background:rgba(255,255,255,.2);padding:3px 10px;border-radius:10px">'
                 '&#128274; Iniciar sesi&oacute;n / Crear cuenta</a>')
    html = HTML.replace("{{USER_BADGE}}", badge)
    resp = make_response(html)
    if not username:
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
        "cfdi-cobro":    base / "input" / "cfdi_cobro",
        "cfdi-pago":     base / "input" / "cfdi_pago",
        "cfdi-facturas": base / "input" / "cfdi_facturas",
        "aux-cobrado":   base / "input" / "aux_cobrado",
        "aux-pagado":    base / "input" / "aux_pagado",
        "pdf-bancos":    base / "input" / "pdf_bancos",
        "aux-bancos":    base / "input" / "aux_bancos",
        "machote":       base / "input" / "machote",
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
    def count_xml(d): return len(list(d.glob("*.xml")) + list(d.glob("*.XML")))
    def count_xls(d): return len(list(d.glob("*.xlsx")) + list(d.glob("*.xls")) + list(d.glob("*.XLSX")))
    def count_pdf(d): return len(list(d.glob("*.pdf")) + list(d.glob("*.PDF")))
    return jsonify({
        "cfdi_cobro":    count_xml(base / "input" / "cfdi_cobro"),
        "cfdi_pago":     count_xml(base / "input" / "cfdi_pago"),
        "cfdi_facturas": count_xml(base / "input" / "cfdi_facturas"),
        "aux_cobrado":   count_xls(base / "input" / "aux_cobrado"),
        "aux_pagado":    count_xls(base / "input" / "aux_pagado"),
        "pdf_bancos":    count_pdf(base / "input" / "pdf_bancos"),
        "aux_bancos":    count_xls(base / "input" / "aux_bancos"),
        "machote":       bool(list((base / "input" / "machote").glob("*.docx"))),
    })


@app.route("/procesar", methods=["POST"])
def procesar():
    """Lanza agente en background; stdout se escribe a progress.log."""
    sid = _sid_from_request()
    if not _check_sid(sid):
        return jsonify({"ok": False}), 400

    with _procs_lock:
        if sid in _procs and _procs[sid].poll() is None:
            return jsonify({"ok": False, "msg": "ya procesando"})

    base     = _session_dir(sid)
    log_path = base / "progress.log"
    # Limpiar log anterior
    log_path.write_text("", encoding="utf-8")

    cmd  = [sys.executable, str(AGENTE_PY), str(base)]
    log_fh = open(log_path, "w", encoding="utf-8", buffering=1)
    proc = subprocess.Popen(
        cmd,
        stdout=log_fh,
        stderr=log_fh,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    # Cerrar el file handle del padre cuando termine el proceso
    def _cleanup(p, fh):
        p.wait()
        fh.close()
    threading.Thread(target=_cleanup, args=(proc, log_fh), daemon=True).start()

    with _procs_lock:
        _procs[sid] = proc
    return jsonify({"ok": True})


@app.route("/estado_proceso")
def estado_proceso():
    """Devuelve si hay un proceso activo y el log hasta ahora."""
    sid = _sid_from_request()
    if not _check_sid(sid):
        return jsonify({"activo": False})
    base     = _session_dir(sid)
    log_path = base / "progress.log"
    with _procs_lock:
        proc = _procs.get(sid)
    activo = proc is not None and proc.poll() is None
    log    = ""
    if log_path.exists():
        try:
            log = log_path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            log = ""
    terminado = not activo and log_path.exists() and len(log) > 0
    return jsonify({"activo": activo, "terminado": terminado, "log": log})


@app.route("/progreso")
def progreso():
    """SSE: lee progress.log y transmite líneas nuevas. Funciona aunque el
    navegador se haya desconectado y vuelto a conectar."""
    sid = _sid_from_request()
    if not _check_sid(sid):
        return Response("", status=400)

    base     = _session_dir(sid)
    log_path = base / "progress.log"

    def _generar():
        # Esperar hasta que exista el log (máx 8 s)
        for _ in range(80):
            if log_path.exists():
                break
            time.sleep(0.1)
        else:
            yield "data: ERROR:No se encontro proceso activo\n\n"
            return

        offset = 0
        done   = False
        while not done:
            try:
                content = log_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                content = ""

            nuevas = content[offset:]
            offset = len(content)

            for linea in nuevas.splitlines():
                linea = linea.strip()
                if not linea:
                    continue
                yield f"data: {linea}\n\n"
                if linea.startswith("RESULTADO:") or linea.startswith("ERROR:"):
                    done = True

            # Verificar si el proceso terminó
            with _procs_lock:
                proc = _procs.get(sid)
            if proc is not None and proc.poll() is not None and not nuevas:
                done = True

            if not done:
                time.sleep(0.4)

        yield "data: DONE\n\n"

    return Response(
        stream_with_context(_generar()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
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


@app.route("/limpiar_zona", methods=["POST"])
def limpiar_zona():
    sid = _sid_from_request()
    if not _check_sid(sid):
        return jsonify({"ok": False}), 400
    datos = request.get_json(force=True, silent=True) or {}
    tipo  = datos.get("tipo", "")
    destdir_map = {
        "cfdi-cobro":    "cfdi_cobro",
        "cfdi-pago":     "cfdi_pago",
        "cfdi-facturas": "cfdi_facturas",
        "aux-cobrado":   "aux_cobrado",
        "aux-pagado":    "aux_pagado",
        "pdf-bancos":    "pdf_bancos",
        "aux-bancos":    "aux_bancos",
        "machote":       "machote",
    }
    if tipo not in destdir_map:
        return jsonify({"ok": False, "error": "tipo invalido"}), 400
    carpeta = _session_dir(sid) / "input" / destdir_map[tipo]
    for f in carpeta.iterdir():
        try:
            if f.is_file():
                f.unlink()
        except Exception:
            pass
    return jsonify({"ok": True})


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
# SAT Descarga Masiva
# ══════════════════════════════════════════════════════════════════════════

def _sat_download_worker(sid: str, base_dir: Path, cer_bytes: bytes,
                         key_bytes: bytes, password: str,
                         fecha_ini, fecha_fin,
                         emitidos: bool, recibidos: bool):
    """Background thread: descarga CFDIs tipo P del SAT usando FIEL,
    luego descarga las facturas tipo I relacionadas (DoctoRelacionado)."""
    import zipfile as _zipfile, io as _io
    import xml.etree.ElementTree as _ET
    import datetime as _dt
    log_path = base_dir / "sat_download.log"

    def _log(msg: str):
        with open(log_path, "a", encoding="utf-8") as _fh:
            _fh.write(msg + "\n")

    _log(f"Inicio descarga SAT: {fecha_ini} a {fecha_fin}")
    try:
        try:
            from satcfdi.models import Signer
            from satcfdi.pacs.sat import SAT, TipoDescargaMasivaTerceros
        except ImportError:
            _log("ERROR:satcfdi no está instalado en este servidor")
            return

        _log("Cargando certificado e.firma...")
        try:
            pwd = password.encode("utf-8") if isinstance(password, str) else password
            signer = Signer.load(certificate=cer_bytes, key=key_bytes, password=pwd)
            _log(f"OK RFC detectado: {signer.rfc}")
        except Exception as exc:
            _log(f"ERROR:No se pudo cargar el certificado e.firma: {exc}")
            return

        sat_svc = SAT(signer=signer)
        _NS_PAG20 = "http://www.sat.gob.mx/Pagos20"
        _NS_TFD   = "http://www.sat.gob.mx/TimbreFiscalDigital"

        def _extract_from_zip(data: bytes, tipo_filter: str | None) -> dict:
            """Extrae XMLs de un ZIP, filtrando por TipoDeComprobante si se indica.
            Retorna {filename: xml_bytes}."""
            result: dict = {}
            try:
                with _zipfile.ZipFile(_io.BytesIO(data)) as zf:
                    for name in zf.namelist():
                        if not name.lower().endswith(".xml"):
                            continue
                        xb = zf.read(name)
                        if tipo_filter:
                            marker_dq = f'TipoDeComprobante="{tipo_filter}"'.encode()
                            marker_sq = f"TipoDeComprobante='{tipo_filter}'".encode()
                            if marker_dq not in xb and marker_sq not in xb:
                                continue
                        result[Path(name).name] = xb
            except Exception as ex2:
                _log(f"ADVERTENCIA ZIP: {ex2}")
            return result

        def _docto_uuids(xml_bytes: bytes) -> set:
            """Extrae UUIDs de DoctoRelacionado de un tipo P."""
            uuids: set = set()
            try:
                root = _ET.fromstring(xml_bytes)
                for dr in root.iter(f"{{{_NS_PAG20}}}DoctoRelacionado"):
                    uid = dr.get("IdDocumento", "")
                    if uid:
                        uuids.add(uid.upper())
            except Exception:
                pass
            return uuids

        def _uuid_from_tfd(xml_bytes: bytes) -> str:
            """Extrae el UUID del TimbreFiscalDigital."""
            try:
                root = _ET.fromstring(xml_bytes)
                for tfd in root.iter(f"{{{_NS_TFD}}}TimbreFiscalDigital"):
                    return tfd.get("UUID", "").upper()
            except Exception:
                pass
            return ""

        all_docto_uuids: set = set()

        # ── Fase 1: CFDIs tipo P ─────────────────────────────────────────────
        if emitidos:
            _log("Solicitando CFDIs emitidos (tipo P) al SAT...")
            cobro_dir = base_dir / "input" / "cfdi_cobro"
            total_e = 0; paq = 0
            try:
                for _, data in sat_svc.recover_comprobante_iwait(
                    fecha_inicial=fecha_ini, fecha_final=fecha_fin,
                    rfc_emisor=signer.rfc,
                    tipo_solicitud=TipoDescargaMasivaTerceros.CFDI
                ):
                    paq += 1
                    xmls = _extract_from_zip(data, "P")
                    for fname, xb in xmls.items():
                        (cobro_dir / fname).write_bytes(xb)
                        all_docto_uuids.update(_docto_uuids(xb))
                        total_e += 1
                    _log(f"Paquete emitidos {paq}: acumulados {total_e} tipo P")
            except Exception as exc:
                _log(f"ADVERTENCIA emitidos: {exc}")
            _log(f"OK Emitidos tipo P: {total_e} en cfdi_cobro/")

        if recibidos:
            _log("Solicitando CFDIs recibidos (tipo P) al SAT...")
            pago_dir = base_dir / "input" / "cfdi_pago"
            total_r = 0; paq = 0
            try:
                for _, data in sat_svc.recover_comprobante_iwait(
                    fecha_inicial=fecha_ini, fecha_final=fecha_fin,
                    rfc_receptor=signer.rfc,
                    tipo_solicitud=TipoDescargaMasivaTerceros.CFDI
                ):
                    paq += 1
                    xmls = _extract_from_zip(data, "P")
                    for fname, xb in xmls.items():
                        (pago_dir / fname).write_bytes(xb)
                        all_docto_uuids.update(_docto_uuids(xb))
                        total_r += 1
                    _log(f"Paquete recibidos {paq}: acumulados {total_r} tipo P")
            except Exception as exc:
                _log(f"ADVERTENCIA recibidos: {exc}")
            _log(f"OK Recibidos tipo P: {total_r} en cfdi_pago/")

        # ── Fase 2: Facturas tipo I relacionadas ─────────────────────────────
        if all_docto_uuids:
            _log(f"Descargando facturas tipo I para {len(all_docto_uuids)} UUIDs relacionados...")
            facturas_dir = base_dir / "input" / "cfdi_facturas"
            facturas_dir.mkdir(parents=True, exist_ok=True)

            # Ampliar rango: 2 años atrás (facturas pueden ser de periodos previos)
            try:
                fac_ini = _dt.date(max(2020, fecha_ini.year - 2), 1, 1)
            except Exception:
                fac_ini = fecha_ini

            total_fac = 0; paq = 0

            for rfc_param, desc in [("rfc_emisor", "emitidas"), ("rfc_receptor", "recibidas")]:
                paq = 0
                try:
                    for _, data in sat_svc.recover_comprobante_iwait(
                        fecha_inicial=fac_ini, fecha_final=fecha_fin,
                        **{rfc_param: signer.rfc},
                        tipo_solicitud=TipoDescargaMasivaTerceros.CFDI
                    ):
                        paq += 1
                        xmls = _extract_from_zip(data, "I")
                        for fname, xb in xmls.items():
                            uuid_fac = _uuid_from_tfd(xb)
                            if uuid_fac in all_docto_uuids:
                                dest = facturas_dir / fname
                                if not dest.exists():
                                    dest.write_bytes(xb)
                                    total_fac += 1
                        _log(f"Facturas {desc} paquete {paq}: {total_fac} matching acumuladas")
                except Exception as exc:
                    _log(f"ADVERTENCIA facturas {desc}: {exc}")

            _log(f"OK Facturas tipo I guardadas: {total_fac} en cfdi_facturas/")
        else:
            _log("Sin UUIDs de DoctoRelacionado — omitiendo descarga tipo I")

        _log("RESULTADO_SAT:OK")

    except Exception as exc:
        import traceback
        _log(f"ERROR:{exc}\n{traceback.format_exc()}")
    finally:
        with _sat_threads_lock:
            _sat_threads.pop(sid, None)


@app.route("/descargar_sat", methods=["POST"])
def descargar_sat():
    sid = _sid_from_request()
    if not _check_sid(sid):
        return jsonify({"ok": False, "error": "sid invalido"}), 400

    with _sat_threads_lock:
        if sid in _sat_threads and _sat_threads[sid].is_alive():
            return jsonify({"ok": False, "error": "Descarga ya en curso"})

    cer_f = request.files.get("cer_file")
    key_f = request.files.get("key_file")
    if not cer_f or not key_f:
        return jsonify({"ok": False, "error": "Falta archivo .cer o .key"}), 400

    password   = request.form.get("password", "")
    fecha_ini_s = request.form.get("fecha_ini", "")
    fecha_fin_s = request.form.get("fecha_fin", "")
    emitidos   = request.form.get("emitidos", "0") == "1"
    recibidos  = request.form.get("recibidos", "0") == "1"

    try:
        import datetime as _dt
        fecha_ini = _dt.date.fromisoformat(fecha_ini_s)
        fecha_fin = _dt.date.fromisoformat(fecha_fin_s)
    except Exception:
        return jsonify({"ok": False, "error": "Fechas inválidas (use YYYY-MM-DD)"}), 400

    cer_bytes = cer_f.read()
    key_bytes = key_f.read()
    base_dir  = _session_dir(sid)

    # Limpiar log anterior
    (base_dir / "sat_download.log").write_text("", encoding="utf-8")

    t = threading.Thread(
        target=_sat_download_worker,
        args=(sid, base_dir, cer_bytes, key_bytes, password,
              fecha_ini, fecha_fin, emitidos, recibidos),
        daemon=True,
    )
    with _sat_threads_lock:
        _sat_threads[sid] = t
    t.start()
    return jsonify({"ok": True})


@app.route("/progreso_sat")
def progreso_sat():
    """SSE: transmite sat_download.log en tiempo real."""
    sid = _sid_from_request()
    if not _check_sid(sid):
        return Response("", status=400)

    base_dir  = _session_dir(sid)
    log_path  = base_dir / "sat_download.log"

    def _generar():
        for _ in range(80):
            if log_path.exists():
                break
            time.sleep(0.1)
        else:
            yield "data: ERROR:No se encontró proceso de descarga activo\n\n"
            return

        offset = 0
        done   = False
        while not done:
            try:
                content = log_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                content = ""

            nuevas = content[offset:]
            offset = len(content)

            for linea in nuevas.splitlines():
                linea = linea.strip()
                if not linea:
                    continue
                yield f"data: {linea}\n\n"
                if linea.startswith("RESULTADO_SAT:") or linea.startswith("ERROR:"):
                    done = True

            # Verificar si el thread terminó
            with _sat_threads_lock:
                vivo = sid in _sat_threads and _sat_threads[sid].is_alive()
            if not vivo and not nuevas:
                done = True

            if not done:
                time.sleep(0.5)

        yield "data: DONE\n\n"

    return Response(
        stream_with_context(_generar()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ══════════════════════════════════════════════════════════════════════════
# Main (desarrollo local)
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import webbrowser, threading as _t
    port = int(os.environ.get("PORT", 5000))
    _t.Timer(1.0, lambda: webbrowser.open(f"http://localhost:{port}")).start()
    app.run(host="0.0.0.0", port=port, debug=False)
