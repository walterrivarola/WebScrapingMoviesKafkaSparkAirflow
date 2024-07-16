# Usar una imagen base oficial de Python
FROM python:3.10

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar los archivos de requerimientos
COPY requirements.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto de la aplicación en el contenedor
COPY . .

# Especificar el comando por defecto para ejecutar FastAPI con Uvicorn
CMD ["uvicorn", "main:app", "--host", "localhost", "--port", "8000"]