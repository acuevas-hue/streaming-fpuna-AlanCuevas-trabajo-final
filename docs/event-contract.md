# Contrato v1

```json
{
  "schema_version": 1,
  "event_id": "demo-1",
  "key": "merchant-A",
  "event_time": "2026-09-14T12:00:10Z",
  "type": "payment",
  "payload": {"amount_minor": 100, "currency": "PYG"}
}
```

| Campo | Regla |
|---|---|
| schema_version | Entero 1; versiones desconocidas se registran como inválidas |
| event_id | Texto no vacío hasta 200 caracteres; estable por hecho de negocio |
| key | Texto no vacío hasta 200 caracteres; coincide con la clave del mensaje Kafka |
| event_time | ISO-8601 UTC terminado en Z; rango 1970–2100; precisión operativa milisegundos |
| type | payment o heartbeat |
| payload | Objeto; para payment, amount_minor entero entre 1 y 10^12 y currency PYG |

PYG se expresa en guaraníes enteros. No usar punto flotante. Cambios aditivos opcionales pueden conservar v1;
un cambio incompatible necesita versión nueva y actualización del consumidor. Los campos adicionales se ignoran.
Un reenvío mantiene todos los campos del hecho original. La clave de dedup efectiva es comercio/ventana/event_id.

Control de progreso: heartbeat conserva los campos básicos, tiene payload `{}` y key `_clock-N`.
Se publica explícitamente en la partición N. Se valida, actualiza el progreso del lector y se filtra antes de agregar.
Solo una fuente coordinada está autorizada a declarar ese progreso; no es un canal de entrada público.

## Salidas

`payment_windows`: merchant_id, window_start UTC, event_count, total_minor, pane_index, timing, updated_at.
Clave primaria compuesta merchant_id/window_start. El total representa el acumulado de pagos admitidos y únicos.
`invalid_events`: error_id SHA-256, detail JSON (key, raw, reason), received_at. Reintentos idénticos no añaden filas.
Los detalles pueden contener datos del evento: la demo es sintética; no introducir datos personales reales.

Generar ejemplo y escenario: `docker compose run --rm producer produce demo`.
La lista exacta y estable está en `Producer.demoEvents()` y sus resultados esperados en `Smoke`.
