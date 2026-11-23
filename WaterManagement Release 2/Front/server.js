// Front de Water Management (Release 2).
// Sirve la carpeta "public" (página del dashboard). La página consulta API_Central
// mediante peticiones HTTP (fetch) a sus endpoints REST.

const express = require('express');
const path = require('path');

const app = express();
const PORT = process.env.WM_FRONT_PORT || 3000;

// Archivos estáticos (index.html, style.css, app.js).
app.use(express.static(path.join(__dirname, 'public')));

app.listen(PORT, () => {
  console.log(`Front escuchando en http://localhost:${PORT}`);
  console.log('La página consulta API_Central (por defecto http://localhost:8080).');
  console.log('Puedes cambiarlo con el parámetro ?api=http://IP:puerto en la URL del navegador.');
});
