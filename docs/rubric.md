# Matriz de evaluación

Esta matriz enlaza implementación y pruebas, no certifica una nota. La evidencia ejecutada está en el workflow
del commit y en su artefacto `streaming-evidence`.

| Criterio | Peso | Implementación / evidencia |
|---|---:|---|
| Caso de uso y arquitectura | 10% | docs/technical.md: problema, usuarios, diagrama y decisiones |
| Eventos y Kafka | 15% | Event.java, Producer.java, compose.yaml, docs/event-contract.md; producer.log |
| Apache Beam | 20% | App.java: KafkaIO; Transforms.java: ParDo, salida lateral, Combine.perKey; Smoke.java |
| Tiempo y ventanas | 15% | DomainTimePolicy.java, Transforms.java; TemporalTest y EventTest |
| Confiabilidad | 15% | SumPayments (unión de IDs), expiración de ventana, Database.UPSERT, retry; smoke.log y restart.log; límites en technical.md |
| Pruebas y E2E | 15% | src/test/, scripts/demo.sh, resultados SQL, workflow y artefactos |
| Documentación y presentación | 10% | README, technical.md, CONTRIBUTORS.md y evidencia de ejecución |

## Casos y oráculos

- Duplicado demo-1: A/12:00 contiene 3 pagos y 600, no 4 y 700.
- Desorden: demo-3 (12:00:20) se publica después de demo-2 (12:00:30) y se incluye en el minuto correcto.
- Late controlado: tras watermark 65 s, evento a 20 s actualiza a 3/600 con pane LATE en TestStream.
- Expirado: tras watermark 181 s, evento a 40 s de 9999 no modifica ese agregado.
- Borde exacto: segundo 60 pertenece a la ventana siguiente; comercio B mantiene estado independiente.
- Inválido: JSON corrupto produce exactamente una fila en invalid_events en la demo.
- Idempotencia: reescritura del agregado y pane antiguo mantienen 3/600 y una única fila por clave.
- Reinicio: Beam vuelve a consumir Kafka mientras PostgreSQL conserva los resultados, que no deben regresar.
