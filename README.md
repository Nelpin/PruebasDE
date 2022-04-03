# PruebasDE

1. Se entregan 3 dataset en la ruta `/assets/*.csv` compuesto de ventas por tienda, tiendas y otras características, la idea es cargar estos datasets para su posterior análisis. Posterior a su cargue debe ser procesado en Python para responder las siguientes preguntas:

- Cual es la tienda con el mayor valor en ventas totales? 20
- Entre las 3 tiendas más grandes cuál es la que más ventas totales registra? 13
- Cual es la tienda con menor ventas ? 33
- Cual es la tienda que mas vendió en el 2 semestre del año 2012? 4

2. Dentro de `/assets/` se encuentra una rchivo llamado `anyclass.py`. Se debe responder lo siguiente:

- Qué hace este código?
Rta: 
* Transforma el método de una clase en una propiedad, este valor se calcula sola  una vez y luego se almacena en caché como un atributo normal y permanece durante el tiempo de   ejecución.
* El código recibe un objeto para una empresa, este objeto se setea y se devuelve como un json parseado, mediante el uso de cached_property se almacena en la cahé para un rápido acceso de esta información.

- Si es posible, refactorizar
* La linea 9 tenía un error, estaba así: attr base = self._factory(instance)
Nota:

Esperamos que el ejercicio sea termindo en 24 horas. Debes hacer fork de la rama y subir tus cambios. Toda la documentacion, explicaciones, print screen, debe ir en el Readme.md.
Por favor enviar correo cuando termines a pda_gobierno@avaldigitallabs.com

![image](https://user-images.githubusercontent.com/7563006/161431422-362d9be3-2246-4ede-aea6-3a4066c2d78f.png)



