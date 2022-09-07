[![Downloads](https://pepy.tech/badge/e-data)](https://pepy.tech/project/e-data)
[![Downloads](https://pepy.tech/badge/e-data/month)](https://pepy.tech/project/e-data)
[![Downloads](https://pepy.tech/badge/e-data/week)](https://pepy.tech/project/e-data)

# python-edata

Este paquete proporciona herramientas para la descarga de tus datos de consumo eléctrico (desde Datadis.es) y su posterior procesado. La motivación principal es que conocer el consumo puede ayudarnos a reducirlo, e incluso a elegir una tarifa que mejor se adapte a nuestras necesidades. A día de hoy sus capacidades de facturación (€) son limitadas, pero algún día pretende ser capaz de simular facturas con reglas de tarificación personalizadas.

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

* **Conectores** (módulo `connectors`), para definir los métodos de consulta a los diferentes proveedores (ahora mismo únicamente se soporta Datadis con el conector `DatadisConnector`).
* **Procesadores** (módulo `processors`), para procesar datos de consumo, maxímetro, o coste (tarificación).
* **Ayudantes** (módulo `helpers`), para ayudar en el uso y gestión de los anteriores, presentando de momento un único ayudante llamado `EdataHelper` que te permite recopilar `X` días de datos y automáticamente procesarlos. También realiza tareas de recuperación ante timeouts o respuestas inválidas.

Estos módulos corresponden a la siguiente estructura del paquete:

```
edata/
    · __init__.py
    · connectors.py
    · processors.py
    · helpers.py
```

## Ejemplo de uso

Partimos de que tenemos credenciales en Datadis.es. Algunas aclaraciones:
* No es necesario solicitar API pública en el registro (se utilizará la API privada habilitada por defecto)
* El username suele ser el NIF del titular
* Copie el CUPS de la web de Datadis, algunas comercializadoras adhieren caracteres adicionales en el CUPS mostrado en su factura.
* La herramienta acepta el uso de NIF autorizado para consultar el suministro de otro titular.

``` python
import logging
from edata.helpers import EdataHelper

# Instanciar el helper
# 'authorized_nif' permite indicar el NIF de la persona que nos autoriza a consultar su CUPS.
# 'data' permite "cargar" al helper datos anteriores (resultado edata.data de una ejecución anterior), para evitar volver a consultar los mismos.
edata = EdataHelper("datadis_username", "datadis_password", "cups", authorized_nif=None, data=None, experimental=False, log_level=logging.INFO)

# Solicitar actualización de todo el histórico (se almacena en edata.data)
edata.update(date_from=datetime(1970, 1, 1), date_to=datetime.today())

# Imprimir info de suministros
print(edata.data["supplies"])

# Imprimir info de contratos
print(edata.data["contracts"])

# Imprimir info de consumos
print(edata.data["consumptions"])

# Imprimir info de maxímetro
print(edata.data["maximeter"])

# Imprimir resumen
print(edata)
```

El contenido de `edata.data` es un diccionario, por lo que podríamos volcarlo en un fichero utilizando cualquier módulo de python (`json.dumps`, `pandas`, etc.).