// Dashboard del Front de Water Management (Release 2).
// Consulta API_Central mediante peticiones HTTP (fetch) y pinta el estado del sistema.

// URL base de API_Central. Se puede sobrescribir con ?api=http://IP:puerto
const params = new URLSearchParams(window.location.search);
const API_BASE = params.get('api') || (window.API_BASE || 'http://localhost:8080');

const INTERVALO_MS = 3000;

const ETIQUETAS_ESTADO = {
  ACTIVADA: 'Available',
  REGANDO: 'Watering',
  FUGA: 'Leak',
  FUERA_SERVICIO: 'Out of Service',
  DESCONECTADA: 'Disconnected',
};

function escapar(texto) {
  const div = document.createElement('div');
  div.textContent = texto == null ? '' : String(texto);
  return div.innerHTML;
}

function pintarEstadoConexion(ok, mensaje) {
  const nodo = document.getElementById('estado');
  if (ok) {
    nodo.textContent = 'Conectado';
    nodo.className = 'estado ok';
  } else {
    nodo.textContent = 'Error';
    nodo.className = 'estado error';
  }
  const caja = document.getElementById('error');
  if (!ok && mensaje) {
    caja.textContent = 'No se pudo conectar con API_Central: ' + mensaje;
    caja.classList.remove('oculto');
  } else {
    caja.classList.add('oculto');
  }
}

function pintarEstaciones(estaciones) {
  const contenedor = document.getElementById('estaciones');
  if (!estaciones || estaciones.length === 0) {
    contenedor.innerHTML = '<div class="vacio">No hay estaciones registradas.</div>';
    return;
  }
  contenedor.innerHTML = estaciones.map((ws) => {
    const estado = (ws.estado || 'DESCONECTADA').toUpperCase();
    const etiqueta = ETIQUETAS_ESTADO[estado] || estado;
    const litros = (ws.litros == null ? 0 : Number(ws.litros)).toFixed(2);
    return `
      <div class="tarjeta ${estado}">
        <div class="id">WS ${escapar(ws.idWs)}</div>
        <div class="ubicacion">${escapar(ws.ubicacion)}</div>
        <div class="estado">${escapar(etiqueta)}</div>
        <div class="meta">Volumen: ${litros} L</div>
        <div class="meta">Última conexión: ${escapar(ws.ultimaConexion) || '—'}</div>
      </div>`;
  }).join('');
}

function pintarOperarios(operarios) {
  const cuerpo = document.getElementById('operarios');
  if (!operarios || operarios.length === 0) {
    cuerpo.innerHTML = '<tr><td colspan="2" class="vacio">Sin operarios.</td></tr>';
    return;
  }
  cuerpo.innerHTML = operarios.map((op) => `
    <tr><td>${escapar(op.idOperario)}</td><td>${escapar(op.nombre)}</td></tr>`).join('');
}

function pintarRiegos(riegos) {
  const cuerpo = document.getElementById('riegos');
  if (!riegos || riegos.length === 0) {
    cuerpo.innerHTML = '<tr><td colspan="5" class="vacio">No hay riegos en curso.</td></tr>';
    return;
  }
  cuerpo.innerHTML = riegos.map((r) => `
    <tr>
      <td>${escapar(r.requestId).slice(0, 8)}…</td>
      <td>${escapar(r.idWs)}</td>
      <td>${escapar(r.idOperario)}</td>
      <td>${escapar(r.duracionSegundos)}</td>
      <td>${escapar(r.estado)}</td>
    </tr>`).join('');
}

function pintarClima(clima) {
  const contenedor = document.getElementById('clima');
  if (!clima || clima.length === 0) {
    contenedor.innerHTML = '<div class="vacio">No hay datos de clima ni alertas.</div>';
    return;
  }
  contenedor.innerHTML = clima.map((c) => {
    const alerta = Boolean(c.alerta);
    const temp = c.temperatura == null ? null : Number(c.temperatura);
    const tempTexto = temp == null ? '—' : (temp - 273.15).toFixed(1) + ' °C';
    const lluvia = Boolean(c.lluvia);
    const motivo = c.motivo || (lluvia ? 'Lluvia' : (alerta ? 'Helada' : 'Sin incidencias'));
    return `
      <div class="clima-card ${alerta ? 'alerta' : 'ok'}">
        <div class="ciudad">${escapar(c.ciudad)}</div>
        <div class="detalle">Temperatura: ${tempTexto}</div>
        <div class="detalle">${lluvia ? 'Lluvia: sí' : 'Lluvia: no'} · ${escapar(motivo)}</div>
        <span class="badge ${alerta ? 'alerta' : 'ok'}">${alerta ? 'ALERTA' : 'OK'}</span>
      </div>`;
  }).join('');
}

async function cargar() {
  try {
    const respuesta = await fetch(`${API_BASE}/api/resumen`);
    if (!respuesta.ok) {
      throw new Error('HTTP ' + respuesta.status);
    }
    const datos = await respuesta.json();
    pintarEstadoConexion(true);
    pintarEstaciones(datos.estaciones);
    pintarOperarios(datos.operarios);
    pintarRiegos(datos.riegos);
    pintarClima(datos.clima);
  } catch (error) {
    pintarEstadoConexion(false, error.message);
  }
}

function actualizarReloj() {
  document.getElementById('reloj').textContent = new Date().toLocaleTimeString();
}

document.addEventListener('DOMContentLoaded', () => {
  cargar();
  actualizarReloj();
  setInterval(cargar, INTERVALO_MS);
  setInterval(actualizarReloj, 1000);
});
