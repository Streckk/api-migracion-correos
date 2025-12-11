+# Guía de integración SSH/SFTP

- +Este documento resume un enfoque recomendado para agregar acceso remoto por SSH/SFTP a la API REST escrita en Rust y SeaORM.
- +## Objetivos
  +- Conectarse a servidores externos vía SSH sin desplegar la API en cada servidor.
  +- Navegar directorios y operar archivos mediante SFTP.
  +- Mantener el código modular para reutilizarlo en otros servicios.
- +## Selección de cliente SSH en Rust
  +- **ssh2**: estable y sencillo (bindings a libssh2). Adecuado para flujos síncronos.
  +- **thrussh / openssh**: más amigables con async; útiles si ya usas `tokio`.
- +## Estructura sugerida del módulo
  +- Crear un servicio, por ejemplo `src/ssh/`, con:
- - `ConnectionConfig` (host, puerto, autenticación, timeouts).
- - `AuthMethod` (password, ruta de clave, clave + passphrase, agente).
- - `SshClient` (trait) con métodos `connect`, `exec`, `list_dir`, `read_file`, `write_file`.
    +- Implementaciones detrás del trait (ej. `Ssh2Client`) para poder cambiar de biblioteca sin tocar los handlers HTTP.
- +## Uso de SFTP
  +- Abrir un canal SFTP tras conectarse para:
- - `readdir` del árbol de directorios objetivo.
- - Descarga/stream de archivos bajo demanda sin copiar todo.
    +- Mantener la lógica de recorrido separada de los handlers; exponer métodos como `list_remote_dir(server_id, path)`.
- +## Concurrencia y ciclo de vida
  +- Para tareas cortas, abrir conexión por solicitud con timeouts razonables.
  +- Para cargas altas, considerar un pool ligero por configuración de servidor (con expiración de inactivos y cierre seguro).
  +- Si el cliente no es `Send + Sync`, envolver en `Arc<Mutex<...>>`.
- +## Seguridad
  +- Priorizar autenticación por clave; permitir password solo si es necesario.
  +- Validar host keys (known_hosts) para evitar MITM.
  +- No registrar secretos; cargar claves desde variables de entorno o gestor de secretos.
  +- Limitar rutas con allowlists o chroot cuando sea viable.
- +## Diseño de API
  +- Endpoints sugeridos:
- - `POST /ssh/connect-test` para validar credenciales/config.
- - `GET /ssh/{server}/list?path=/ruta` para listar directorios.
- - `GET /ssh/{server}/file?path=...` para descargar/stream de archivos.
    +- Guardar las configuraciones de servidor en la base de datos y resolverlas en la capa de servicio; evita enviar credenciales crudas en cada petición.
- +## Errores y observabilidad
  +- Normalizar errores SSH/SFTP hacia el tipo de error de la API con mensajes claros (fallo de auth, host inaccesible, path inexistente).
  +- Agregar logs estructurados (host, path, operación, duración) y métricas (éxitos/fallos) para depurar.
- +## Pruebas
  +- Usa un contenedor SSH local en CI (p.ej., `linuxserver/openssh-server`) para pruebas de integración.
  +- Mockea el trait `SshClient` en pruebas unitarias para no requerir conexiones reales en los handlers.
