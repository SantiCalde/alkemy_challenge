# alkemy_challenge

## Arranquemos por lo basico:
¿Qué se necesita para hacer un deploy del proyecto?
  Muy facil... Esta es una guia de 5 sencillos pasos
  1) Hace un fork del repo a tu pc.
  2) Genera e Inicia tu ambiente virtual. 
      Inicia un ambiente virtual con el comando
      (py -m venv venv) <- puede cambiar segun sistemas operativos consultar con soporte al cliente
      ¡IMPORTANTISIMO! No te olvides de iniciar tu ambiente virtual como le pasa al amigo de un amigo...
      (venv\Scripts\activate.bat)
  3) Instala las dependencias necesarias. (volve a revisar que tu ambiente este activadooo)
      (pip install -r requirements.txt)
  4) Genera un archivo llamado .env en la carpeta raiz del proyecto, este te va ayudar a tener encapsuladas del resto del prorama las credenciales de tu base de datos.
  5) Copia esta lista de declaraciones dentro de tu archivo para luego rellenarlas con tus credenciales. Lo que si te recomiendo que dejes tal cual esta es DB_ENGINE
     a menos que por algun motivo quieras cambiar de base de datos.
     
     DB_ENGINE=postgresql+psycopg2
     DB_HOST=localhost
     DB_USER=postgres
     DB_PASSWORD=passejemplo
     DB_PORT=5432
     DB_NAME=alkemy_challenge_bbdd
   
  6) Ejecuta el archivo main.py YA TENES TU PIPELINE CORRIENDO FELICIDADESS!!!
  
  ## Caso especiales:
  ¿Qué hago si por algun motivo cambian las url de las paginas?
    Tranquilo, esta todo pensado, dentro del modulo 'ETL_module' se encuentra un submodulo llamado 'extract'. Lo unico que tenes que hacer es dirigirte a este 
    submodulo, entrar al archivo *webs.yaml* y modificar las antiguas URLs por las nuevas. :D
    
  ¿Tengo que tener instalado PostgreSQL para ejecutarlo?
    Sip, tenes que tenerlo instalado y ademas tenes q tener creada dentro de el una base de datos (¿¿¿Que credenciales pusiste si tenes esta predunta??? necesito signos de pregunta mas grandes...)
