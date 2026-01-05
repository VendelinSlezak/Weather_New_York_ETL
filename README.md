# ELT proces datasetu Weather New York

Tento repozitár obsahuje projekt pre predmet Databázové technológie. Jeho cieľom bolo transformovať dáta zo Snowflake Marketplace do hviezdicovej schémy cez ELT proces. Ako podkladové dáta bol vybraný dataset "Snowpark for Python - Hands-on-Lab - Weather Data by Pelmorex Weather Source". Tento dataset obsahuje dáta o počasí v New Yorku od roku 2013. Projekt sa zameriava na preskúmanie týchto dát s cieľom poskytnúť analýzu dát o počasí v New Yorku, ktoré sa dajú využiť pri rozhodovaní ktoré s ním súvisí.

## Úvod a popis zdrojových dát

Dáta sú v datasete poskytované skrze dve tabuľky:

* `history_day` - obsahuje údaje o minulých nameraných hodnotách počasia
* `forecast_day` - obsahuje predpoveď počasia pre new york

Vzhľadom na účel projektu sme pracovali iba s tabuľkou `history_day`, ktorá má nasledujúci formát:

// odkaz na obrázok tabuľky

## Dimenzionálny model

Dáta boli spracované podľa hviezdicovej schémy (star schémy) ktorá obsahuje jednu tabuľku faktov (facts_measurement) ktorá je spojená s nasledujúcimi šiestimi dimenziami:

* `dim_location` - obsahuje údaje o lokácií (krajina, PSČ)
* `dim_date` - obsahuje údaje o dátume (rok, mesiac, deň, ročné obdobie)
* `dim_temperature` - obsahuje údaje o teplote (minimálna, priemerná, maximálna)
* `dim_humidity` - obsahuje údaje o vlhkosti (minimálna, priemerná, maximálna)
* `dim_pressure`- obsahuje údaje o tlaku (minimálna, priemerná, maximálna)
* `dim_precipitation` - obsahuje údaje o zrážkach (celkové, snehové)

Vzájoné prepojenie tabuliek je vizuálne znázornené na tomto entitno-relačnom diagrame:

// odkaz na obrázok star schémy

## ELT proces v Snowflake

ELT proces je zložený z troch častí: Extract (extrahovanie) / Load (nahrávanie) / Transform (transformovanie). Dáta boli spracované týmto spôsobom, aby sa s nimi dalo lepšie pracovať.

### Extract (extrahovanie dát)

Nakoľko za podkladový dataset sme použili dataset zo snowflake marketplace, tento krok už bol za nás vykonaný poskytovateľmi údajov.

### Load (nahrávanie dát)

Dáta z datasetu boli v tomto kroku nahrané do staging tabuľky. Keďže sme nepotrebovali úplne všetky údaje z pôvodného datasetu, do staging tabuľky boli vybrané iba tie relevantné. Staging tabuľka bola vytvorená a naplnená údajmi týmto SQL:

```
CREATE TABLE history_day_staging AS SELECT
    POSTAL_CODE,
    COUNTRY,
    DATE_VALID_STD,
    MIN_TEMPERATURE_AIR_2M_C,
    AVG_TEMPERATURE_AIR_2M_C,
    MAX_TEMPERATURE_AIR_2M_C,
    MIN_HUMIDITY_RELATIVE_2M_PCT,
    AVG_HUMIDITY_RELATIVE_2M_PCT,
    MAX_HUMIDITY_RELATIVE_2M_PCT,
    MIN_PRESSURE_2M_MB,
    AVG_PRESSURE_2M_MB,
    MAX_PRESSURE_2M_MB,
    TOT_PRECIPITATION_MM,
    TOT_SNOWFALL_CM
FROM SNOWPARK_FOR_PYTHON__HANDSONLAB__WEATHER_DATA_BY_PELMOREX_WEATHER_SOURCE.ONPOINT_ID.HISTORY_DAY;
```

### Transform (transformácia dát)

V tomto kroku boli dáta transformované do hviezdicovej schémy, boli vyčistené a taktiež obohatené o niektoré údaje. Dimenzie boli navrhnuté vzhľadom na faktovú tabuľku, ktorej riadok vždy popisuje dáta jednodenného merania. Nakoľko počasie v minulosti je nemenné, všetky dimenzie sú typu `SCD 0`.

#### dim_location

```
CREATE TABLE dim_location AS SELECT
    DISTINCT CONCAT(COUNTRY, POSTAL_CODE) AS locationId,
    POSTAL_CODE,
    COUNTRY
FROM history_day_staging;
```

Táto dimenzia popisuje lokáciu v ktorej boli údaje o počasí namerané.

#### dim_date

```
CREATE TABLE dim_date AS SELECT
    DISTINCT DATE_VALID_STD AS dateId,
    YEAR(DATE_VALID_STD) AS year,
    MONTH(DATE_VALID_STD) AS month,
    DAY(DATE_VALID_STD) AS day,
    CASE 
        WHEN MONTH(DATE_VALID_STD) IN (12, 1, 2) THEN 'winter'
        WHEN MONTH(DATE_VALID_STD) IN (3, 4, 5) THEN 'spring'
        WHEN MONTH(DATE_VALID_STD) IN (6, 7, 8) THEN 'summer'
        WHEN MONTH(DATE_VALID_STD) IN (9, 10, 11) THEN 'autumn'
    END AS season
FROM history_day_staging;
```

Táto dimenzia popisuje dátum merania. Obohacuje ho o ročné obdobie v ktorom sa nachádza.

#### dim_temperature

```
CREATE TABLE dim_temperature AS SELECT
    DISTINCT CONCAT(MIN_TEMPERATURE_AIR_2M_C, AVG_TEMPERATURE_AIR_2M_C, MAX_TEMPERATURE_AIR_2M_C) AS temperatureId,
    MIN_TEMPERATURE_AIR_2M_C AS min,
    AVG_TEMPERATURE_AIR_2M_C AS avg,
    MAX_TEMPERATURE_AIR_2M_C AS max
FROM history_day_staging;
```

Táto dimenzia popisuje údaje o teplote v daný deň.

#### dim_humidity

```
CREATE TABLE dim_humidity AS SELECT
    DISTINCT CONCAT(MIN_HUMIDITY_RELATIVE_2M_PCT, AVG_HUMIDITY_RELATIVE_2M_PCT, MAX_HUMIDITY_RELATIVE_2M_PCT) AS humidityId,
    MIN_HUMIDITY_RELATIVE_2M_PCT AS min,
    AVG_HUMIDITY_RELATIVE_2M_PCT AS avg,
    MAX_HUMIDITY_RELATIVE_2M_PCT AS max,
FROM history_day_staging;
```

Táto dimenzia popisuje údaje o vlhkosti v daný deň.

#### dim_pressure

```
CREATE TABLE dim_pressure AS SELECT
    DISTINCT CONCAT(MIN_PRESSURE_2M_MB, AVG_PRESSURE_2M_MB, MAX_PRESSURE_2M_MB) AS pressureId,
    MIN_PRESSURE_2M_MB AS min,
    AVG_PRESSURE_2M_MB AS avg,
    MAX_PRESSURE_2M_MB AS max,
FROM history_day_staging;
```

Táto dimenzia popisuje údaje o tlaku v daný deň.

#### dim_precipitation

```
CREATE TABLE dim_precipitation AS SELECT
    DISTINCT CONCAT(TOT_PRECIPITATION_MM, TOT_SNOWFALL_CM) AS precipitationId,
    TOT_PRECIPITATION_MM AS total,
    TOT_SNOWFALL_CM AS snowfall
FROM history_day_staging;
```

Táto dimenzia popisuje údaje o zrážkach v daý deň.

#### facts_measurement

```
CREATE TABLE facts_measurement AS SELECT
    ROW_NUMBER() OVER (ORDER BY DATE_VALID_STD) AS measurementId,
    DATE_VALID_STD AS dateId,
    CONCAT(COUNTRY, POSTAL_CODE) AS locationId,
    CONCAT(MIN_TEMPERATURE_AIR_2M_C, AVG_TEMPERATURE_AIR_2M_C, MAX_TEMPERATURE_AIR_2M_C) AS temperatureId,
    CONCAT(MIN_HUMIDITY_RELATIVE_2M_PCT, AVG_HUMIDITY_RELATIVE_2M_PCT, MAX_HUMIDITY_RELATIVE_2M_PCT) AS humidityId,
    CONCAT(MIN_PRESSURE_2M_MB, AVG_PRESSURE_2M_MB, MAX_PRESSURE_2M_MB) AS pressureId,
    CONCAT(TOT_PRECIPITATION_MM, TOT_SNOWFALL_CM) AS precipitationId,

    TOT_PRECIPITATION_MM - AVG(TOT_PRECIPITATION_MM) OVER (
        PARTITION BY locationId
        ORDER BY DATE_VALID_STD ASC
        RANGE BETWEEN INTERVAL '30 DAYS' PRECEDING AND CURRENT ROW
    ) AS precipitationDiffFromAvg30,
    AVG_PRESSURE_2M_MB - AVG(AVG_PRESSURE_2M_MB) OVER (
        PARTITION BY locationId
        ORDER BY DATE_VALID_STD ASC
        RANGE BETWEEN INTERVAL '30 DAYS' PRECEDING AND CURRENT ROW
    ) AS pressureDiffFromAvg30
FROM history_day_staging;
```

Táto tabuľka faktov prepája všetky vyššie uvedné dimenzie na konkrétne jednodňové merania. Sú v nej taktiež predrátané dve agregačné hodnoty `precipitationDiffFromAvg30` ktorá uvádza nakoľko sa množstvo zrážok líšilo od priemeru zrážok posledných tridsiatich dní a `pressureDiffFromAvg30` ktorá uvádza nakoľko sa priemerný denný tlak líšil od priemerného tlaku posledných tridsiatich dní.

Pre optimalizovanie úložiska bola na záver staging tabuľka ostránená:

```
DROP TABLE IF EXISTS history_day_staging;
```
