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
```