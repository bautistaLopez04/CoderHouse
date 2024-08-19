from extraccion import extraerDatos
from carga import cargarDatos

def main():
    # Extraer datos de la API
    datos_extraidos = extraerDatos()
    
    if datos_extraidos:
        # Cargar datos en la base de datos
        cargarDatos(datos_extraidos)
        print("Los datos se han cargado correctamente en la base de datos.")
    else:
        print("No se pudieron extraer datos de la API.")

if __name__ == "__main__":
    main()
