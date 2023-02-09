[![Downloads](https://pepy.tech/badge/e-data)](https://pepy.tech/project/e-data)
[![Downloads](https://pepy.tech/badge/e-data/month)](https://pepy.tech/project/e-data)
[![Downloads](https://pepy.tech/badge/e-data/week)](https://pepy.tech/project/e-data)

# python-edata

Este paquete proporciona herramientas para la descarga de tus datos de consumo eléctrico (desde Datadis.es) y su posterior procesado. La motivación principal es que conocer el consumo puede ayudarnos a reducirlo, e incluso a elegir una tarifa que mejor se adapte a nuestras necesidades. A día de hoy sus capacidades de facturación (€) son limitadas, soporta PVPC (según disponibilidad de datos de REData) y tarificación fija por tramos. Es el corazón de la integración [homeassistant-edata](https://github.com/uvejota/homeassistant-edata).

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

El paquete consta de tres módulos diferenciados:

* **Conectores** (módulo `connectors`), para definir los métodos de consulta a los diferentes proveedores: Datadis y REData.
* **Procesadores** (módulo `processors`), para procesar datos de consumo, maxímetro, o coste (tarificación). Ahora mismo consta de tres procesadores: `billing`, `consumption` y `maximeter`, además de algunas utilidades ubicadas en `utils`. Los procesadores deben heredar de la clase Processor definida en `base.py`
* **Ayudantes** (módulo `helpers`), para ayudar en el uso y gestión de los anteriores, presentando de momento un único ayudante llamado `EdataHelper` que te permite recopilar `X` días de datos (por defecto 365) y automáticamente procesarlos. Los datos son almacenados en la variable `data`, mientras que los atributos autocalculados son almacenados en la variable `attributes`. Por lo general, primero utilizan los conectores y luego procesan los datos, gestionando varias tareas de recuperación (principalmente para Datadis).

Estos módulos corresponden a la siguiente estructura del paquete:

```
edata/
    · __init__.py
    · connectors/
        · __init__.py
        · datadis.py
        · redata.py
    · processors/
        · __init__.py
        · base.py
        · billing.py
        · consumption.py
        · maximeter.py
        · utils.py
    · helpers.py
```

## Ejemplo de uso

Partimos de que tenemos credenciales en Datadis.es. Algunas aclaraciones:
* No es necesario solicitar API pública en el registro (se utilizará la API privada habilitada por defecto)
* El username suele ser el NIF del titular
* Copie el CUPS de la web de Datadis, algunas comercializadoras adhieren caracteres adicionales en el CUPS mostrado en su factura.
* La herramienta acepta el uso de NIF autorizado para consultar el suministro de otro titular.

``` python
from datetime import datetime
import json

# importamos definiciones de datos que nos interesen
from edata.definitions import PricingRules
# importamos el ayudante
from edata.helpers import EdataHelper
# importamos el procesador de utilidades
from edata.processors import utils

# Preparar reglas de tarificación (si se quiere)
PRICING_RULES_PVPC = PricingRules(
    p1_kw_year_eur=30.67266,
    p2_kw_year_eur=1.4243591,
    meter_month_eur=0.81,
    market_kw_year_eur=3.113,
    electricity_tax=1.0511300560,
    iva_tax=1.05,
    # podemos rellenar los siguientes campos si quisiéramos precio fijo (y no pvpc)
    p1_kwh_eur=None,
    p2_kwh_eur=None,
    p3_kwh_eur=None,
)

# Instanciar el helper
# 'authorized_nif' permite indicar el NIF de la persona que nos autoriza a consultar su CUPS.
# 'data' permite "cargar" al helper datos anteriores (resultado edata.data de una ejecución anterior), para evitar volver a consultar los mismos.
edata = EdataHelper(
            "datadis_user",
            "datadis_password",
            "cups",
            datadis_authorized_nif=None,
            pricing_rules=PRICING_RULES_PVPC, # si se le pasa None, no aplica tarificación
            data=None, # aquí podríamos cargar datos anteriores
        )

# Solicitar actualización de todo el histórico (se almacena en edata.data)
edata.update(date_from=datetime(1970, 1, 1), date_to=datetime.today())

# volcamos todo lo obtenido a un fichero
with open("backup.json", "w") as file:
    json.dump(utils.serialize_dict(edata.data), file) # se puede utilizar deserialize_dict para la posterior lectura del backup

# Imprimir atributos
print(edata.attributes)
```
