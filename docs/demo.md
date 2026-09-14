# Guion de demostración y presentación (5–7 minutos)

1. Problema (45 s): operador que necesita pagos/minuto por comercio, con reenvíos y desorden en la red.
2. Arquitectura (60 s): mostrar diagrama; señalar KafkaIO real, timestamps, dedup y materialización SQL.
3. Ejecución (2 min): preparar imágenes antes de presentar y ejecutar `bash scripts/demo.sh`.
   Mostrar `producer.log`: demo-1 repetido y timestamp 20 después de 30; mostrar SQL 3/600, 1/700, 1/900.
   Mostrar el JSON inválido separado y `SMOKE PASS`. La construcción inicial puede durar más que el video;
   explicarlo si se recorta la espera, conservando evidencia completa.
4. Tiempo (60 s): abrir TemporalTest y su reporte. Watermark 65, late a 20 y expirado después de 181.
   Explicar diferencia entre fuera de orden y late. Heartbeats avanzan progreso en particiones inactivas.
5. Fallos (45 s): mostrar restart.log y UPSERT de pane viejo; describir reconstrucción de estado desde Kafka.
6. Límites (45 s): una réplica, DirectRunner sin checkpoints, pagos inmutables, retención, no exactly-once E2E.

## Preguntas de defensa

- ¿Por qué 60 s y 120 s? Resolución operativa y margen de corrección; más margen aumenta estado y demora finalización.
- ¿Kafka ordena event_time? No; ordena append por partición, y la fuente puede producir timestamps desordenados.
- ¿Por qué no basta la idempotencia del productor? Los reenvíos de negocio son envíos nuevos para Kafka.
- ¿Por qué count y no pane_index? El índice puede reiniciarse; count protege acumulados positivos bajo el contrato.
- ¿Qué pasa si una partición queda inactiva? Retiene watermark hasta el heartbeat; no se usa now() para históricos.
- ¿Qué pasa tras perder todo Kafka? Se necesita regenerar el dataset; no hay recuperación productiva garantizada.

## Entrega pendiente del integrante

Elegir video breve o demostración en vivo. Si se graba video, añadir aquí el enlace real.
Revisar las contribuciones declaradas y poder explicar el código. No se ha generado un video automáticamente.
