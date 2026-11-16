# WM_Registry — Release 2

Servicio REST HTTPS para alta, baja, consulta y autenticacion de estaciones WS.

## Endpoints

- `PUT /api/registry/ws/{id}` JSON: `{"location":"Jardin central"}`. Registra o actualiza la estacion y devuelve una credencial aleatoria. La credencial se devuelve solo en esta respuesta.
- `GET /api/registry/ws/{id}`. Consulta los datos del registro, sin exponer la credencial.
- `POST /api/registry/ws/{id}/authenticate` JSON: `{"credential":"..."}`. Comprueba la credencial.
- `DELETE /api/registry/ws/{id}`. Da de baja la estacion e invalida su credencial.

## Dependencias e integracion

El modulo usa Java 11, Jackson y SQLite JDBC. Comparte el fichero SQLite `watermanagement.db` con WM_Central; por eso ambos servicios deben montar el mismo volumen `/data`. El Registry añade a la tabla `WS` los campos `credencial_hash` y `registrada`. Nunca guarda la credencial original, solo su SHA-256.

## Crear certificado local/autofirmado

Ejecutar en una maquina segura, sustituyendo `IP_PUBLICA` y/o `DNS_PUBLICO` por el host real al que se conectara el Monitor:

```bash
keytool -genkeypair -alias wm-registry -keyalg RSA -keysize 3072 \
  -storetype PKCS12 -keystore registry.p12 -validity 3650 \
  -dname "CN=wm-registry, OU=SD, O=WaterManagement, L=Alicante, C=ES" \
  -ext "SAN=ip:IP_PUBLICA,dns:DNS_PUBLICO"
```

Exportar el certificado publico para instalarlo como certificado de confianza en el equipo donde se ejecute WM_WS_M:

```bash
keytool -exportcert -alias wm-registry -keystore registry.p12 \
  -storetype PKCS12 -rfc -file registry.crt
```

No subir `registry.p12`, contrasenas ni claves privadas a GitHub. El certificado publico `registry.crt` se puede distribuir al PC de laboratorio para configurar la confianza TLS.

## Variables de entorno

- `WM_REGISTRY_HOST` (por defecto `0.0.0.0`)
- `WM_REGISTRY_PORT` (por defecto `8443`)
- `WM_DB_PATH` (por defecto `database/watermanagement.db`)
- `WM_REGISTRY_KEYSTORE` (por defecto `secrets/registry.p12`)
- `WM_REGISTRY_KEYSTORE_PASSWORD` (obligatoria)

## Compilar

```bash
mvn -B -DskipTests package
```

## Ejecutar

```bash
WM_REGISTRY_KEYSTORE_PASSWORD='contrasena-del-keystore' \
WM_REGISTRY_KEYSTORE='/ruta/segura/registry.p12' \
WM_DB_PATH='/data/watermanagement.db' \
java -jar target/wm-registry-1.0.0.jar
```

La aplicacion no arranca en HTTP plano: exige un keystore para servir HTTPS.
