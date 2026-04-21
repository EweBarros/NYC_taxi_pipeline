-- Query 2: Média de passageiros (passenger_count) por hora do dia, Maio 2023, todos os táxis
--
-- "Todos os táxis" = Yellow + Green. FHV e FHVHV não possuem passenger_count
-- e ficam apenas no Bronze (decisão documentada no README).
--
-- Filtramos passenger_count > 0: dados NYC TLC contêm ~123k registros com valor 0 ou NULL
-- por erro de input do motorista. Incluí-los distorceria a média para baixo.
-- O filtro é aplicado apenas aqui; a Gold preserva os dados originais.
--
-- Pré-requisito: executar build_gold_trips() antes desta query.

SELECT
    HOUR(pickup_datetime)          AS hour_of_day,
    ROUND(AVG(passenger_count), 2) AS avg_passenger_count
FROM nyc_taxi.main.gold_trips
WHERE pickup_datetime  >= '2023-05-01'
  AND pickup_datetime   < '2023-06-01'
  AND passenger_count  IS NOT NULL
  AND passenger_count   > 0
GROUP BY HOUR(pickup_datetime)
ORDER BY hour_of_day;
