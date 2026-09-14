# Monitoreo de pagos por comercio — Trabajo final

**Integrante:** Alan Cuevas ([acuevas-hue](https://github.com/acuevas-hue)).
Curso: Streaming de datos y sus aplicaciones, Maestría en Inteligencia Artificial — FPUNA.

Pipeline real: productor sintético → Apache Kafka → KafkaIO / Apache Beam Java → PostgreSQL.
Calcula cantidad y monto de pagos PYG por comercio y minuto, con deduplicación y actualizaciones idempotentes.

## Estado y evidencia

Consultar [GitHub Actions](https://github.com/acuevas-hue/streaming-fpuna-AlanCuevas-trabajo-final/actions).
El workflow ejecuta las pruebas y la demostración real; el artefacto `streaming-evidence` contiene los reportes y logs.
La existencia de este código no prueba una ejecución exitosa: comprobar que el workflow del commit que se entrega esté verde.

## Requisitos

- Docker Engine con Docker Compose v2 y acceso a Internet para descargar imágenes y dependencias.
- Bash; reservar aproximadamente 4 GB de RAM y 6 GB de disco. La primera construcción puede tardar varios minutos.
- Java 17 y Maven 3.9 si se quieren ejecutar las pruebas fuera de Docker.
- Kafka y PostgreSQL no exponen puertos al host; todos los comandos operan dentro de Compose.

## Demostración completa

```bash
git clone https://github.com/acuevas-hue/streaming-fpuna-AlanCuevas-trabajo-final.git
cd streaming-fpuna-AlanCuevas-trabajo-final
bash scripts/demo.sh
```

Crea un entorno de demostración aislado, construye la aplicación, inicia Kafka y PostgreSQL, produce el escenario,
verifica resultados, reinicia Beam para probar replay y guarda evidencia en `evidence/`.
No borra entornos anteriores. El nombre del entorno queda en `evidence/compose-project.txt`.

Resultados esperados para 2026-09-14 UTC:

| Comercio | Inicio de ventana | Pagos únicos | Total PYG |
|---|---|---:|---:|
| merchant-A | 12:00:00 | 3 | 600 |
| merchant-B | 12:00:00 | 1 | 700 |
| merchant-A | 12:01:00 | 1 | 900 |

Se envían seis mensajes de pago: cinco únicos y un duplicado. El pago de las 12:00:20 llega después del de las 12:00:30.
Hay además un JSON inválido y tres heartbeats de progreso. `invalid_events` debe contener una fila.
La evidencia tardía respecto al watermark y el descarte por expiración se prueban separadamente con `TestStream`.
El smoke test demuestra desorden real desde Kafka; no etiqueta ese desorden como late sin medir el watermark.

Inspeccionar y detener el entorno generado:

```bash
export COMPOSE_PROJECT_NAME="$(cat evidence/compose-project.txt)"
docker compose logs -f pipeline
docker compose exec postgres psql -U payments -d payments -c 'TABLE payment_windows;'
docker compose down
```

`down` conserva el volumen PostgreSQL. Para eliminar **solo los datos de esta demo**, usar `docker compose down -v` con el mismo nombre de proyecto.

## Operación manual y productor continuo

```bash
docker compose up -d --build pipeline
docker compose run --rm producer produce demo
docker compose run --rm verify
docker compose logs -f pipeline
docker compose down
```

En un entorno separado del escenario verificado:

```bash
export COMPOSE_PROJECT_NAME=fpuna-live
docker compose up -d --build pipeline
docker compose run --rm producer produce live 42 1000 2026-09-14T14:00:00Z
```

El productor continúa hasta Ctrl+C. Argumentos: semilla, intervalo real en milisegundos y base temporal UTC.
Con los mismos tres argumentos reproduce los mismos eventos/IDs y orden; el reloj de dominio avanza por índice.
Cada décimo pago se duplica y cada séptimo lleva 15 s de retraso de dominio. Emite progreso en todas las particiones.
No ejecutar `verify` sobre datos live: sus aserciones corresponden exclusivamente al dataset demo.

## Pruebas

```bash
mvn -B -ntp verify
# Alternativa sin Java/Maven instalados en el host:
docker run --rm -v "$PWD:/work" -w /work maven:3.9.9-eclipse-temurin-17 mvn -B -ntp verify
bash scripts/demo.sh
```

Pruebas de contrato, agregación, overflow, watermark, validación lateral, ventanas, aislamiento,
duplicados, late data y expiración. El smoke test verifica SQL real, reintentos, pane antiguo y replay tras reinicio.

## Documentación

- [Documento técnico y arquitectura](docs/technical.md)
- [Contrato y ejemplos](docs/event-contract.md)
- [Matriz de rúbrica y evidencia](docs/rubric.md)
- [Guion de demostración y presentación](docs/demo.md)
- [Integrantes y contribuciones](CONTRIBUTORS.md)

## Garantías resumidas

Ventanas fijas de 60 s; allowed lateness 120 s; watermark por partición = máximo tiempo de dominio válido menos 5 s.
Dedup por `(key, ventana, event_id)`. Panes acumulativos. PostgreSQL usa clave `(merchant_id, window_start)` y acepta
solo incrementos del conteo acumulado para evitar regresiones por reintentos o replay parcial.

No se afirma exactly-once end-to-end. DirectRunner no conserva estado tras reinicio: reconstruye desde Kafka.
Esto requiere retener todo el historial necesario y mantener eventos inmutables. Una sola instancia del pipeline y
un solo productor lógico por escenario; no es una plataforma de pagos de producción.
