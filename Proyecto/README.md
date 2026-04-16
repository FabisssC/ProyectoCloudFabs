# Proyecto Final — Ingeniería de Datos con Python

## 1. Descripción general

Este proyecto consiste en el diseño e implementación de un **pipeline de ingeniería de datos end-to-end**, utilizando **Python** y **Google Cloud Platform (GCP) o Amazon Web Services (AWS) o Microsoft Azure**, siguiendo una arquitectura **Bronze / Silver / Gold**.

El pipeline integra **múltiples fuentes de datos reales**, aplica procesos de **ETL**, y almacena los resultados finales en la nube, listos para análisis y toma de decisiones.

---
## 2. Objetivo del proyecto

Diseñar y desarrollar un pipeline de datos que:

- Integre múltiples fuentes heterogéneas
- Aplique transformaciones consistentes y reproducibles
- Almacene los datos en la nube de forma eficiente
- Pueda ejecutarse de forma automatizada
- Refleje buenas prácticas profesionales de Ingeniería de Datos
---
## 3. Resumen del problema 
Este proyecto implementa un pipeline de datos con arquitectura Medallion para integrar múltiples fuentes heterogéneas relacionadas con transacciones bancarias, clientes y comercios. El objetivo es consolidar los datos, aplicar procesos de limpieza y transformación consistentes, y publicar un conjunto final de métricas listas para análisis de fraude financiero en la nube.

## 2. Arquitectura lógica



### Entidades mínimas del proyecto

1. Transacciones
Representa cada operación realizada por una tarjeta en un comercio.
Campos mínimos: card_id, merchant_id, purchase_date, purchase_amount,
installments, authorized_flag y variables categóricas/geográficas relevantes.

2. Merchants
Representa atributos del comercio asociados a las transacciones.
Campos mínimos: merchant_id, merchant_category_id, subsector_id,
city_id y state_id.

3. Tipo de cambio
Representa la tasa de conversión diaria entre monedas.
Campos mínimos: rate_date, base_currency, target_currency,
exchange_rate y provider.