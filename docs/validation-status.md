# Estado de validación — primera implementación

Fecha: 2026-09-14.

## Ejecutado en el entorno de desarrollo

- Parser Java 17 (`JavacTask.parse`) sobre los nueve archivos Java: sin errores de sintaxis.
- `bash -n scripts/demo.sh`: sin errores de sintaxis.
- Parseo YAML de Compose y workflow; parseo XML de pom.xml: correcto.
- Revisión estática de contrato, escenario esperado, SQL, rutas de documentación y archivos del paquete.

El parseo Java NO resuelve dependencias ni comprueba tipos contra Apache Beam. No equivale a compilar.

## Pendiente

- Descargar dependencias y compilar con Maven.
- Ejecutar 17 tests JUnit, incluidas pruebas Beam TestStream.
- Construir imagen Docker.
- Ejecutar smoke test real KafkaIO/Beam/PostgreSQL y verificar reinicio.
- Publicar y ejecutar GitHub Actions.
- Demostración en vivo o video, y completar contribuciones personales.

## Bloqueos observados

Maven y Docker no están instalados en el entorno de desarrollo disponible.
La API de GitHub permitió consultar el repositorio, pero el intento de crear README.md devolvió
HTTP 403: `Resource not accessible by integration`. No se publicó ningún archivo ni se ejecutó CI.
No se dispone de un commit remoto de esta implementación.

## Validación al habilitar el entorno

```bash
mvn -B -ntp verify
bash scripts/demo.sh
```

Corregir cualquier error real que aparezca antes de declarar la entrega validada. El workflow ya contiene
esos pasos y guarda los reportes/evidencias incluso cuando falla. No se debe reemplazar esta comprobación
por una simulación en memoria ni por pruebas debilitadas.
