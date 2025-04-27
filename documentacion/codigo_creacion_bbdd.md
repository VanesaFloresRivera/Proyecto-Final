```sql
CREATE TABLE continente (
    id_continente SERIAL PRIMARY KEY,
    nombre_continente TEXT UNIQUE
);

CREATE TABLE pais_destino (
    id_pais_destino SERIAL PRIMARY KEY,
    nombre_pais_destino TEXT UNIQUE,
    id_continente INT REFERENCES continente(id_continente) ON DELETE CASCADE
);

CREATE TABLE itinerario (
    id_itinerario SERIAL PRIMARY KEY,
    detalle_itinerario TEXT UNIQUE
);

CREATE TABLE ciudad (
    id_ciudad SERIAL PRIMARY KEY,
    nombre_ciudad TEXT UNIQUE
);

CREATE TABLE ciudad_itinerario (
    id_ciudad_itinerario SERIAL PRIMARY KEY,
    id_ciudad INT REFERENCES ciudad(id_ciudad) ON DELETE CASCADE,
    id_itinerario INT REFERENCES itinerario(id_itinerario) ON DELETE CASCADE
);

CREATE TABLE nombre_opcion (
    id_nombre_opcion SERIAL PRIMARY KEY,
    nombre_opcion TEXT UNIQUE
);

CREATE TABLE en_ultimo_escrapeo (
    id_en_escrapeo SERIAL PRIMARY KEY,
    opcion_en_escrapeo BOOLEAN
);

CREATE TABLE viaje (
    id_viaje SERIAL PRIMARY KEY,
    url_viaje TEXT UNIQUE,
    duracion_dias INT,
    duracion_noches INT,
    id_itinerario INT REFERENCES itinerario(id_itinerario) ON DELETE CASCADE,
    id_en_escrapeo INT REFERENCES en_ultimo_escrapeo(id_en_escrapeo) ON DELETE CASCADE
);

CREATE TABLE nombre_viaje (
    id_nombre_viaje SERIAL PRIMARY KEY,
    nombre_viaje TEXT UNIQUE,
    id_viaje INT REFERENCES viaje(id_viaje) ON DELETE CASCADE
);

CREATE TABLE precio_viaje (
    id_registro_precio_viaje SERIAL PRIMARY KEY,
    precio_viaje INT,
    fecha_precio_viaje DATE,
    id_viaje INT REFERENCES viaje(id_viaje) ON DELETE CASCADE
);

CREATE TABLE combinacion_destino_viaje (
    id_registro_combinacion_destino_viaje SERIAL PRIMARY KEY,
    id_viaje INT REFERENCES viaje(id_viaje) ON DELETE CASCADE,
    id_pais_destino INT REFERENCES pais_destino(id_pais_destino) ON DELETE CASCADE
);

CREATE TABLE opcion (
    id_opcion SERIAL PRIMARY KEY,
    url_opcion TEXT UNIQUE,
    id_nombre_opcion INT REFERENCES nombre_opcion(id_nombre_opcion) ON DELETE CASCADE,
    id_en_escrapeo INT REFERENCES en_ultimo_escrapeo(id_en_escrapeo) ON DELETE CASCADE,
    id_viaje INT REFERENCES viaje(id_viaje) ON DELETE CASCADE
);

CREATE TABLE precio_opcion (
    id_registro_precio_opcion SERIAL PRIMARY KEY,
    precio_opcion INT,
    fecha_precio_opcion DATE,
    id_opcion INT REFERENCES opcion(id_opcion) ON DELETE CASCADE
);

CREATE TABLE ccaa_origen (
    id_ccaa SERIAL PRIMARY KEY,
    nombre_ccaa TEXT UNIQUE
);

CREATE TABLE turismo_emisor (
    id_registro_turismo SERIAL PRIMARY KEY,
    id_ccaa INT REFERENCES ccaa_origen(id_ccaa) ON DELETE CASCADE,
    id_pais_destino INT REFERENCES pais_destino(id_pais_destino) ON DELETE CASCADE,
    num_turistas INT,
    num_pernoctaciones INT,
    año INT,
    mes INT
);


```