-- Query 1: Média do valor total (total_amount) por mês, Yellow Taxis, Jan-Mai 2023
--
-- Filtramos total_amount > 0: dados NYC TLC contêm ~25k registros com valor negativo
-- (payment_type 4 = disputas de tarifa). Incluí-los distorceria a média para baixo.
-- O filtro é aplicado apenas aqui; a Gold preserva os dados originais.
--
-- Pré-requisito: executar build_gold_trips() antes desta query.

SELECT
    YEAR(pickup_datetime)        AS year,
    MONTH(pickup_datetime)       AS month,
    ROUND(AVG(total_amount), 2)  AS avg_total_amount
FROM nyc_taxi.gold.trips
WHERE taxi_type        = 'yellow'
  AND pickup_datetime >= '2023-01-01'
  AND pickup_datetime  < '2023-06-01'
  AND total_amount     > 0
GROUP BY YEAR(pickup_datetime), MONTH(pickup_datetime)
ORDER BY YEAR(pickup_datetime), MONTH(pickup_datetime);
