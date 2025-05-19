```sql
--
SELECT pd.nombre_pais_destino AS pais_destino, 
	pd.nombre_continente AS continente,
	te.ano, 
	te.mes, 
	sum(num_turistas) AS suma_turistas,
	sum(num_pernoctaciones) AS suma_pernoctaciones
FROM turismo_emisor te 
INNER JOIN pais_destino pd ON te.id_pais_destino =pd.id_pais_destino
GROUP BY 1,2,3,4
ORDER BY te.ano, te.mes;

SELECT pd.nombre_pais_destino AS pais_destino, 
	pd.nombre_continente AS continente,
	sum(num_turistas) AS suma_turistas,
	sum(num_pernoctaciones) AS suma_pernoctaciones,
	(sum(num_pernoctaciones)/sum(num_turistas)) AS promedio_pernoctaciones_turismo_emisor,
	count(DISTINCT v.id_viaje) AS num_viajes_ofertados_tui,
	round(avg(v.duracion_noches),0) AS promedio_pernoctaciones_viajes_tui	
FROM turismo_emisor te 
INNER JOIN pais_destino pd ON te.id_pais_destino =pd.id_pais_destino
INNER JOIN combinacion_destino_viaje cdv ON pd.id_pais_destino =cdv.id_pais_destino
INNER JOIN viaje v ON cdv.id_viaje = v.id_viaje 
GROUP BY 1,2
ORDER BY pais_destino;


SELECT pd.nombre_pais_destino AS pais_destino, 
	pd.nombre_continente AS continente,
	te.ano, 
	sum(num_turistas) AS suma_turistas,
	sum(num_pernoctaciones) AS suma_pernoctaciones,
	(sum(num_pernoctaciones)/sum(num_turistas)) AS promedio_pernoctaciones_turismo_emisor,
	count(DISTINCT v.id_viaje) AS num_viajes_ofertados_tui,
	round(avg(v.duracion_noches),0) AS promedio_pernoctaciones_viajes_tui	
FROM turismo_emisor te 
INNER JOIN pais_destino pd ON te.id_pais_destino =pd.id_pais_destino
INNER JOIN combinacion_destino_viaje cdv ON pd.id_pais_destino =cdv.id_pais_destino
INNER JOIN viaje v ON cdv.id_viaje = v.id_viaje 
GROUP BY 1,2,3
ORDER BY pais_destino, te.ano;

SELECT pd.nombre_continente AS continente,
	pd.nombre_pais_destino AS pais_destino, 
	count(DISTINCT v.id_viaje) AS num_viajes_ofertados_tui,
	round(avg(v.duracion_noches),0) AS promedio_pernoctaciones_viajes_tui	
FROM pais_destino pd
INNER JOIN combinacion_destino_viaje cdv ON pd.id_pais_destino =cdv.id_pais_destino
INNER JOIN viaje v ON cdv.id_viaje = v.id_viaje 
GROUP BY 1,2
ORDER BY pais_destino;

SELECT pd.nombre_continente AS continente,
	pd.nombre_pais_destino AS pais_destino, 
	count(DISTINCT v.id_viaje) AS num_viajes_ofertados_tui,
	round(avg(v.duracion_noches),0) AS promedio_pernoctaciones_viajes_tui	
FROM pais_destino pd
INNER JOIN ciudad c  ON pd.id_pais_destino =c.id_pais_destino
INNER JOIN ciudad_itinerario ci ON c.id_ciudad =ci.id_ciudad
INNER JOIN itinerario i ON ci.id_itinerario = i.id_itinerario
INNER JOIN viaje v ON i.id_itinerario  = v.id_itinerario 
GROUP BY 1,2
ORDER BY pais_destino;

SELECT pd.nombre_continente AS continente,
	pd.nombre_pais_destino AS pais_destino, 
	v.url_viaje AS nombre_viaje_ofertado_tui,
	v.duracion_noches AS pernoctaciones_tui
FROM pais_destino pd
INNER JOIN ciudad c  ON pd.id_pais_destino =c.id_pais_destino
INNER JOIN ciudad_itinerario ci ON c.id_ciudad =ci.id_ciudad
INNER JOIN itinerario i ON ci.id_itinerario = i.id_itinerario
INNER JOIN viaje v ON i.id_itinerario  = v.id_itinerario
ORDER BY pais_destino,nombre_viaje_ofertado_tui ;


--

-- Consulta por paises con los viajes demandados por el turismo Español y el num. viajes ofertados por TUI con la localización de la pag. web para ellos.

WITH cte_turismo_emisor_resumen AS (
  SELECT 
    id_pais_destino,
    SUM(num_turistas) AS suma_turistas,
    SUM(num_pernoctaciones) AS suma_pernoctaciones
  FROM turismo_emisor
  GROUP BY id_pais_destino
)

SELECT pd.nombre_continente AS continente,
	pd.nombre_pais_destino AS pais_destino, 
  te.suma_turistas,
  te.suma_pernoctaciones,
   (te.suma_pernoctaciones / te.suma_turistas) AS promedio_pernoctaciones_turismo_emisor,
	count(DISTINCT v.id_viaje) AS num_viajes_ofertados_tui,
	round(avg(v.duracion_noches),0) AS promedio_pernoctaciones_viajes_tui	
FROM cte_turismo_emisor_resumen te 
LEFT JOIN pais_destino pd ON te.id_pais_destino =pd.id_pais_destino
LEFT JOIN combinacion_destino_viaje cdv ON pd.id_pais_destino =cdv.id_pais_destino
LEFT JOIN viaje v ON cdv.id_viaje = v.id_viaje 
GROUP BY 1,2,3,4,5
ORDER BY pais_destino;




-- Consulta por paises con los viajes demandados por el turismo Español y el num. viajes ofertados por TUI pero teniendo en cuenta las ciudades:

--CREO TABLA TEMPORAL CON LA SUMA DEL TURISMO EMISOR:

WITH cte_turismo_emisor_resumen AS (
  SELECT 
    id_pais_destino,
    SUM(num_turistas) AS suma_turistas,
    SUM(num_pernoctaciones) AS suma_pernoctaciones
  FROM turismo_emisor
  GROUP BY id_pais_destino
)


SELECT pd.nombre_continente AS continente,
	pd.nombre_pais_destino AS pais_destino, 
  te.suma_turistas,
  te.suma_pernoctaciones,
   (te.suma_pernoctaciones / te.suma_turistas) AS promedio_pernoctaciones_turismo_emisor,
	count(DISTINCT v.id_viaje) AS num_viajes_ofertados_tui,
	round(avg(v.duracion_noches),0) AS promedio_pernoctaciones_viajes_tui	
FROM turismo_emisor_resumen te 
LEFT JOIN pais_destino pd ON te.id_pais_destino =pd.id_pais_destino
LEFT JOIN ciudad c  ON pd.id_pais_destino =c.id_pais_destino
LEFT JOIN ciudad_itinerario ci ON c.id_ciudad =ci.id_ciudad
LEFT JOIN itinerario i ON ci.id_itinerario = i.id_itinerario
LEFT JOIN viaje v ON i.id_itinerario  = v.id_itinerario 
GROUP BY 1,2,3,4,5
ORDER BY pais_destino;

--Consulta por paises con los viajes ofertados por tui y los demandados por el turismo Español teniendo en cuenta las ciudades:

--CREO TABLA TEMPORAL CON LA SUMA DEL TURISMO EMISOR:

WITH cte_turismo_emisor_resumen AS (
  SELECT 
    id_pais_destino,
    SUM(num_turistas) AS suma_turistas,
    SUM(num_pernoctaciones) AS suma_pernoctaciones
  FROM turismo_emisor
  GROUP BY id_pais_destino
)

SELECT pd.nombre_continente AS continente,
	pd.nombre_pais_destino AS pais_destino, 
	count(DISTINCT v.id_viaje) AS num_viajes_ofertados_tui,
	round(avg(v.duracion_noches),0) AS promedio_pernoctaciones_viajes_tui,
  (te.suma_pernoctaciones / te.suma_turistas) AS promedio_pernoctaciones_turismo_emisor,
  te.suma_turistas,
  te.suma_pernoctaciones
FROM viaje v 
LEFT JOIN itinerario i ON v.id_itinerario = i.id_itinerario
LEFT JOIN ciudad_itinerario ci  ON i.id_itinerario = ci.id_ciudad 
LEFT JOIN ciudad c ON ci.id_ciudad = c.id_ciudad
LEFT JOIN pais_destino pd ON c.id_pais_destino = pd.id_pais_destino
LEFT JOIN turismo_emisor_resumen te ON pd.id_pais_destino = te.id_pais_destino
GROUP BY 1,2,5,6,7
ORDER BY pais_destino ASC;


--Consulta por paises con los viajes ofertados por tui y los demandados por el turismo Español según están registrados en la página de TUI:
--CREO TABLA TEMPORAL CON LA SUMA DEL TURISMO EMISOR:

WITH cte_turismo_emisor_resumen AS (
  SELECT 
    id_pais_destino,
    SUM(num_turistas) AS suma_turistas,
    SUM(num_pernoctaciones) AS suma_pernoctaciones
  FROM turismo_emisor
  GROUP BY id_pais_destino
)

-- EJECUTO LA CONSULTA
SELECT 
  pd.nombre_continente AS continente,
  pd.nombre_pais_destino AS pais_destino, 
  COUNT(DISTINCT v.id_viaje) AS num_viajes_ofertados_tui,
  ROUND(AVG(v.duracion_noches), 0) AS promedio_pernoctaciones_viajes_tui,
  (te.suma_pernoctaciones / te.suma_turistas) AS promedio_pernoctaciones_turismo_emisor,
  te.suma_turistas,
  te.suma_pernoctaciones
FROM viaje v
LEFT JOIN combinacion_destino_viaje cdv ON v.id_viaje = cdv.id_viaje
LEFT JOIN pais_destino pd ON cdv.id_pais_destino = pd.id_pais_destino
LEFT JOIN turismo_emisor_resumen te ON pd.id_pais_destino = te.id_pais_destino
GROUP BY 1, 2, 5, 6, 7
ORDER BY pais_destino;


SELECT te.id_pais_destino,
	sum(te.num_turistas) AS suma_turistas
FROM turismo_emisor te
INNER JOIN pais_destino pd ON te.id_pais_destino = te.id_pais_destino
GROUP BY 1
ORDER BY suma_turistas;

--Extraccion de las tablas
-- TABLA PAIS_DESTINO
SELECT *
FROM pais_destino;


-- TABLA VIAJE CON PAIS Y CONTINENTE SEGUN ESTÁ EN LA PAGINA WEB
SELECT pd.*,
	v.*
FROM viaje v
INNER JOIN combinacion_destino_viaje cdv ON v.id_viaje = cdv.id_viaje
INNER JOIN pais_destino pd ON cdv.id_pais_destino =pd.id_pais_destino;

-- TABLA VIAJE CON PAIS,CONTINENTES Y CIUDADES LA RELACION CON LAS CIUDADES
SELECT pd.*,
	v.*,
	c.id_ciudad,
	c.nombre_ciudad
FROM viaje v
INNER JOIN itinerario i ON v.id_itinerario = i.id_itinerario
INNER JOIN ciudad_itinerario ci ON i.id_itinerario = ci.id_itinerario
INNER JOIN ciudad c ON ci.id_ciudad = c.id_ciudad
INNER JOIN pais_destino pd ON c.id_pais_destino =pd.id_pais_destino;

--TABLA VIAJE CON PRECIOS
SELECT v.*,
	precio_viaje,
	fecha_precio_viaje
FROM viaje v
INNER JOIN precio_viaje pv ON v.id_viaje = pv.id_viaje; 

--TABLA ID VIAJES CON CAMBIOS DE PRECIOS
SELECT v.id_viaje,
	count(DISTINCT precio_viaje) as num_precios_ofertados
FROM viaje v
INNER JOIN precio_viaje pv ON v.id_viaje = pv.id_viaje
GROUP BY v.id_viaje
HAVING count(DISTINCT precio_viaje)>1;

--TABLA TURISMO EMISOR INDICANDO EL PAIS
SELECT pd.*,
	ccaa_origen,
	num_turistas,
	num_pernoctaciones,
	ano,
	mes
FROM turismo_emisor te 
INNER JOIN pais_destino pd ON te.id_pais_destino = pd.id_pais_destino
ORDER BY ano,mes ;

```