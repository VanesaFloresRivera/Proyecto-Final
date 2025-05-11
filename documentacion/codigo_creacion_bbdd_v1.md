```sql
CREATE TABLE pais_destino (
    id_pais_destino SERIAL PRIMARY KEY,
    nombre_pais_destino TEXT UNIQUE,
    nombre_continente TEXT
);

CREATE TABLE itinerario (
    id_itinerario INT PRIMARY KEY
);

CREATE TABLE ciudad (
    id_ciudad SERIAL PRIMARY KEY,
    nombre_ciudad TEXT UNIQUE,
    id_pais_destino INT REFERENCES pais_destino(id_pais_destino) ON DELETE CASCADE
);

CREATE TABLE ciudad_itinerario (
    id_ciudad INT REFERENCES ciudad(id_ciudad) ON DELETE CASCADE,
    id_itinerario INT REFERENCES itinerario(id_itinerario) ON DELETE CASCADE,
    PRIMARY KEY (id_ciudad, id_itinerario)
);

CREATE TABLE viaje (
    id_viaje SERIAL PRIMARY KEY,
    nombre_viaje TEXT UNIQUE,
    url_viaje TEXT UNIQUE,
    duracion_dias INT,
    duracion_noches INT,
    id_itinerario INT REFERENCES itinerario(id_itinerario) ON DELETE CASCADE,
    viaje_activo BOOLEAN
);

CREATE TABLE precio_viaje (
    id_registro_precio_viaje SERIAL PRIMARY KEY,
    precio_viaje INT,
    fecha_precio_viaje DATE,
    id_viaje INT REFERENCES viaje(id_viaje) ON DELETE CASCADE
);

CREATE TABLE combinacion_destino_viaje (
    id_viaje INT REFERENCES viaje(id_viaje) ON DELETE CASCADE,
    id_pais_destino INT REFERENCES pais_destino(id_pais_destino) ON DELETE CASCADE,
    PRIMARY KEY (id_viaje, id_pais_destino)
);

CREATE TABLE turismo_emisor (
    id_registro_turismo SERIAL PRIMARY KEY,
    ccaa_origen TEXT,
    id_pais_destino INT REFERENCES pais_destino(id_pais_destino) ON DELETE CASCADE,
    num_turistas INT,
    num_pernoctaciones INT,
    ano INT,
    mes INT
);
```