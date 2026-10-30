#Dockerfile es el manual de instrucciones que utiliza Docker para construir una imagen.
# Usar una imagen oficial de Node.js como base
FROM node:18-alpine

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

#Copia los archivos de definición de dependencias (package.json y package-lock.json) desde el proyecto al contenedor.
# Copiar archivos de dependencias e instalarlas
COPY package*.json ./
RUN npm install

# Copiar el resto de los archivos de la aplicación como los archivos .html, .js ....
COPY . .