[![Downloads](https://pepy.tech/badge/e-data)](https://pepy.tech/project/e-data)
[![Downloads](https://pepy.tech/badge/e-data/month)](https://pepy.tech/project/e-data)
[![Downloads](https://pepy.tech/badge/e-data/week)](https://pepy.tech/project/e-data)

# python-edata

Este paquete proporciona herramientas para la descarga de tus datos de consumo eléctrico (desde Datadis.es) y su posterior procesado. La motivación principal es que conocer el consumo puede ayudarnos a reducirlo, e incluso a elegir una tarifa que mejor se adapte a nuestras necesidades. Soporta facturación con PVPC (según disponibilidad de datos de REData) y tarificación fija personalizable mediante fórmulas. Todos los datos se almacenan localmente en una base de datos SQLite. Es el corazón de la integración [homeassistant-edata](https://github.com/uvejota/homeassistant-edata).

_**Esta herramienta no mantiene ningún tipo de vinculación con los proveedores de datos anteriormente mencionados, simplemente consulta la información disponible y facilita su posterior análisis.**_

## Instalación

Puedes instalar la última versión estable mediante:

``` bash
pip install e-data
```

Si quieres probar la versión `dev` o contribuir a su desarrollo, clona este repositorio e instala manualmente las dependencias:

``` bash
pip install -r requirements.txt
```

## Estructura

El paquete consta de varios módulos:

* **Proveedores** (`providers`): Conectores para consultar diferentes fuentes de datos.
  - `datadis.py`: Conector para la API privada de Datadis (datos de consumo, potencia, contratos y suministros).
  - `redata.py`: Conector para la API pública de REData (precios PVPC en tiempo real).

* **Servicios** (`services`): Capa de lógica de negocio que gestiona la sincronización y procesamiento de datos.
  - `data_service.py`: Servicio para gestionar datos de telemetría (consumo, potencia, estadísticas).
  - `bill_service.py`: Servicio para gestionar la facturación y cálculo de costes.

* **Modelos** (`models`): Definiciones de estructuras de datos usando Pydantic.
  - `supply.py`: Modelos de suministros (`Supply`) y contratos (`Contract`).
  - `data.py`: Modelos de datos de telemetría (`Energy`, `Power`, `Statistics`).
  - `bill.py`: Modelos de facturación (`Bill`, `EnergyPrice`, `BillingRules`, `PVPCBillingRules`).

* **Base de datos** (`database`): Gestión de persistencia local con SQLite.
  - `controller.py`: Controlador principal de la base de datos (`EdataDB`).
  - `models.py`: Modelos SQLModel para las tablas de la base de datos.
  - `queries.py`: Consultas SQL predefinidas.

* **Utilidades** (`core`): Funciones auxiliares para cálculos de tarifas, fechas, etc.

* **CLI** (`cli.py`): Interfaz de línea de comandos para operaciones básicas.

## Ejemplo de uso

### Usando los servicios (recomendado)

Partimos de que tenemos credenciales en [Datadis.es](https://datadis.es). Algunas aclaraciones:
* No es necesario solicitar API pública en el registro (se utilizará la API privada habilitada por defecto)
* El username suele ser el NIF del titular
* Copie el CUPS de la web de Datadis, algunas comercializadoras adhieren caracteres adicionales en el CUPS mostrado en su factura
* La herramienta acepta el uso de NIF autorizado para consultar el suministro de otro titular

``` python
import asyncio
from datetime import datetime
from edata.services.data_service import DataService
from edata.services.bill_service import BillService
from edata.models.bill import PVPCBillingRules

async def main():
    # Crear el servicio de datos
    data_service = DataService(
        cups="ES0000000000000000XX",  # Tu CUPS
        datadis_user="12345678A",     # Tu NIF/usuario
        datadis_pwd="tu_password",
        storage_path="./my_data"      # Directorio para la BD 
        datadis_authorized_nif=None,  # NIF autorizado (opcional)
    )
    
    # Actualizar todos los datos disponibles
    await data_service.update()
    
    # Obtener suministros y contratos
    supplies = await data_service.get_supplies()
    contracts = await data_service.get_contracts()
    
    # Obtener datos de consumo y potencia
    energy = await data_service.get_energy()
    power = await data_service.get_power()
    
    # Obtener estadísticas agregadas (diarias o mensuales)
    daily_stats = await data_service.get_statistics("day")
    monthly_stats = await data_service.get_statistics("month")
    
    # Crear el servicio de facturación
    bill_service = BillService(
        cups="ES0000000000000000XX",
        storage_path="./my_data"  # Mismo directorio que data_service
    )
    
    # Definir reglas de facturación PVPC (valores por defecto españoles)
    pvpc_rules = PVPCBillingRules(
        p1_kw_year_eur=30.67266,      # Término potencia P1 (€/kW/año)
        p2_kw_year_eur=1.4243591,     # Término potencia P2 (€/kW/año)
        meter_month_eur=0.81,          # Alquiler contador (€/mes)
        market_kw_year_eur=3.113,      # Otros cargos (€/kW/año)
        electricity_tax=1.0511300560,  # Impuesto eléctrico
        iva_tax=1.21                   # IVA (21%)
    )
    
    # Actualizar facturación con PVPC
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    await bill_service.update(start_date, end_date, billing_rules=pvpc_rules, is_pvpc=True)
    
    # Obtener facturas calculadas
    bills = await bill_service.get_bills(start_date, end_date, type_="hour")
    daily_bills = await bill_service.get_bills(start_date, end_date, type_="day")
    monthly_bills = await bill_service.get_bills(start_date, end_date, type_="month")
    
    # Mostrar resumen
    total_cost = sum(b.value_eur for b in bills)
    print(f"Coste total: {total_cost:.2f} €")
    print(f"Consumo total: {sum(e.value_kWh for e in energy):.2f} kWh")

# Ejecutar
asyncio.run(main())
```

### Usando la CLI

El paquete incluye una interfaz de línea de comandos para operaciones básicas:

``` bash
# Ver suministros disponibles
python -m edata.cli show-supplies <username>

# Descargar todos los datos de un CUPS
python -m edata.cli download-all --cups ES0000000000000000XX <username>

# Actualizar facturación con tarifa fija
python -m edata.cli update-bill \
  --cups ES0000000000000000XX \
  --p1-kw-year-eur 30.67 \
  --p2-kw-year-eur 1.42 \
  --p1-kwh-eur 0.15 \
  --p2-kwh-eur 0.10 \
  --p3-kwh-eur 0.08 \
  --meter-month-eur 0.81
```
