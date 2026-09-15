# Validación ejecutada

Revisado el 2026-09-15. El bloqueo inicial de escritura quedó resuelto.

## Ejecución verificada

- Commit de implementación: `ca3ae87ff88f14e1dfe395d93802b4ffad467781`.
- [GitHub Actions aprobado](https://github.com/acuevas-hue/streaming-fpuna-AlanCuevas-trabajo-final/actions/runs/34890876248).
- [Reportes y evidencia descargable](https://github.com/acuevas-hue/streaming-fpuna-AlanCuevas-trabajo-final/actions/runs/34890876248/artifacts/10366718212).
- Maven: compilación y empaquetado aprobados.
- JUnit: **19 pruebas, 0 fallos, 0 errores, 0 omitidas** (16 EventTest y 3 TemporalTest).
- Imagen Docker construida y ejecutada con Kafka y PostgreSQL reales.
- Smoke test fuente → Kafka → KafkaIO/Beam → PostgreSQL aprobado.
- Reinicio de Beam con replay desde Kafka aprobado, manteniendo resultados SQL.

## Evidencia temporal registrada

```text
TEMPORAL A|0|2|300 timing=EARLY
TEMPORAL A|0|2|300 timing=ON_TIME
TEMPORAL A|0|3|600 timing=LATE
```

El escenario incluye un duplicado y un evento posterior a la expiración de la ventana.
La aserción verifica que ninguno incremente indebidamente el agregado.

## Salida real del smoke test

| Comercio | Ventana UTC del 2026-09-14 | Pagos | Total PYG |
|---|---|---:|---:|
| merchant-A | 12:00:00 | 3 | 600 |
| merchant-A | 12:01:00 | 1 | 900 |
| merchant-B | 12:00:00 | 1 | 700 |

Una fila en `invalid_events`. La reescritura de un resultado y de un pane antiguo no modificó
los totales. La segunda verificación tras reiniciar Beam produjo exactamente la misma salida.

## Reproducción

```bash
mvn -B -ntp verify
bash scripts/demo.sh
```

Consultar siempre la ejecución del commit que se entrega. Los artefactos de Actions tienen retención limitada;
descargarlos antes de la entrega. La referencia anterior identifica una ejecución concreta, no garantiza
que cualquier modificación futura mantenga los mismos resultados.

## Pendientes personales para la entrega

- Presentar una demostración en vivo o grabar el video breve.
- Completar y confirmar las contribuciones personales en CONTRIBUTORS.md.
- Preparar la defensa siguiendo docs/demo.md.

La ejecución automatizada es evidencia técnica; no reemplaza la presentación ni la defensa del integrante.
