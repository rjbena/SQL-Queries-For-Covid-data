--Working with covid data to demonstrate sql skills

--Opening tables to ensure they were imported 
SELECT COUNT(DiSTINCT[location])
FROM owid_covid_data_deaths$


SELECT
    *
FROM gdp_data_deaths$
WHERE income_type <> 'World'
SELECT top 5
    *
FROM continent_data_deaths$

SELECT top 5
    *
FROM continent_data_vacc$

SELECT top 5
    *
FROM gdp_data_deaths$

SELECT top 5
    *
FROM gdp_data_vacc$

SELECT top 5
    *
FROM european_union_deaths$

SELECT top 5
    *
FROM european_union_vacc$

--Looking at what our data is

SELECT COUNT(DISTINCT location) as number_of_locations, COUNT(DISTINCT date) as days_counted, COUNT(DISTINCT continent) as number_of_continents, SUM(population) as population_count
FROM owid_covid_data_deaths$

SELECT COUNT(DISTINCT location) as number_of_locations, COUNT(DISTINCT date) as days_counted, COUNT(DISTINCT continent) as number_of_continents
FROM owid_covid_data_vacc$


SELECT COUNT(DISTINCT date) as days_counted, COUNT(DISTINCT continent) as number_of_continents, SUM(population) as population_count
FROM continent_data_deaths$

SELECT COUNT(DISTINCT cdd.date) as continent_date_count, COUNT(DISTINCT ocdd.date) as owid_date_count, COUNT(DISTINCT ocdd.date) - COUNT(DISTINCT cdd.date) as date_count_diff
FROM continent_data_deaths$ as cdd, owid_covid_data_deaths$ as ocdd

--Continent data does not start till Jan 22 2020, OWID data starts Jan 1 2020 accounts for the missing 21 days of data counted
SELECT DISTINCT date as con_dates
FROM continent_data_deaths$

SELECT DISTINCT date as owid_dates
FROM owid_covid_data_deaths$

--Clean way to find dates not in continent_data_deaths
SELECT DISTINCT ocdd.date
FROM owid_covid_data_deaths$ as ocdd
    LEFT JOIN continent_data_deaths$ as cdd
    ON ocdd.date = cdd.[date]
WHERE cdd.date IS NULL

-- Find data from Jan 1 2020 to Jan 22 2020
SELECT *
FROM owid_covid_data_deaths$
WHERE date BETWEEN '2020-01-01 00:00:00' AND '2020-01-21'

-- Summerize data by continant 
SELECT continent, SUM(DISTINCT(population))
FROM owid_covid_data_deaths$
GROUP BY continent

-- Continent data summary 
SELECT *
--INTO newest_continent_data
FROM continent_data_deaths$
WHERE NOT EXISTS (
       SELECT *
FROM continent_data_deaths$ as cdd
WHERE continent_data_deaths$.Continent = cdd.Continent AND cdd.date > continent_data_deaths$.[date]
   )

--Check out new table
SELECT *
FROM newest_continent_data

-- Find last date recorded for each location
SELECT *
FROM (
    SELECT continent, [location], date, population, total_cases, total_deaths, ROW_NUMBER() over(partition by location ORDER BY date desc) as rn
    FROM owid_covid_data_deaths$
) t
WHERE t.rn = 1
ORDER BY t.[date]

-- Other version of above query 

SELECT *
--INTO #newest_location_data
FROM owid_covid_data_deaths$

    INNER JOIN (
        SELECT [location], MAX(date) as date
    FROM owid_covid_data_deaths$
    GROUP BY [location] 
    ) ocdd ON owid_covid_data_deaths$.[location] = ocdd.[location] AND owid_covid_data_deaths$.[date] = ocdd.[date]

-- Similar Query but into temp query
SELECT *
INTO #newest_owid_data
FROM owid_covid_data_deaths$
WHERE NOT EXISTS (
    SELECT *
FROM owid_covid_data_deaths$ as ocdd
WHERE ocdd.[location] = owid_covid_data_deaths$.[location] AND ocdd.date > owid_covid_data_deaths$.date

)
-- Inspect temp table
SELECT *
FROM ..#newest_owid_data
-- Questions to answer

-- What locations has the highest percentage of their population infected
SELECT *
FROM (
        SELECT continent, [location], date, population, total_cases, total_deaths, ROUND((total_cases/ population) * 100,2) AS percent_infected,
        ROUND((CAST(total_deaths AS float)/population) * 100,2) AS percent_dead, ROUND((CAST(total_deaths AS float)/ total_cases) * 100,2) AS percent_infected_dead,
        ROW_NUMBER() over(PARTITION BY location ORDER BY date desc) as rn
    FROM owid_covid_data_deaths$ 
    ) t
WHERE t.rn = 1
ORDER BY t.percent_infected desc, t.percent_dead desc, t.percent_infected_dead desc

-- What day had they highest number of new infections

SELECT date, SUM(CAST(new_cases as int)) AS new_cases_on_day
FROM owid_covid_data_deaths$
GROUP BY date
ORDER BY new_cases_on_day DESC

-- Day with the highest number of new deaths

SELECT date, SUM(CAST(new_deaths as int)) as deaths_on_day
FROM owid_covid_data_deaths$
GROUP BY date
ORDER BY deaths_on_day DESC

-- What day for each location had the highest number of  deaths
SELECT *
FROM (
    SELECT continent, [location], date, new_deaths, ROW_NUMBER() OVER (PARTITION BY location ORDER BY new_deaths DESC) as rn
    FROM owid_covid_data_deaths$
) t
WHERE t.rn = 1
ORDER BY t.[location]
-- What month had the highest number of infections. 
SELECT MONTH(date) as month , SUM(CAST(new_cases as int)) as monthly_cases
FROM owid_covid_data_deaths$
GROUP BY MONTH(date)
ORDER BY 2 DESC
-- Sum the location information to find information about the continent
SELECT continent, SUM(DISTINCT(population)) as population, SUM(CAST(new_cases as int)) as new_cases, SUM(CAST(new_deaths as int)) as new_deaths, MAX(CAST(total_deaths as int)) as total_deaths
FROM owid_covid_data_deaths$
WHERE date BETWEEN '2020-01-01 00:00:00' AND '2020-01-21'
GROUP BY continent
ORDER BY continent

-- Show number of vaccinations and next to new_cases and deaths
SELECT *
FROM owid_covid_data_vacc$ ocdv
    JOIN owid_covid_data_deaths$ ocdd
    ON ocdv.[date] = ocdd.[date] AND ocdv.[location] = ocdd.[location]
WHERE people_vaccinated IS NOT NULL
ORDER BY ocdv.people_vaccinated

-- Total population vs vaccinations

SELECT ocdv.[location], ocdv.[date], ocdd.population, ocdd.total_cases, ocdd.total_deaths, ocdv.total_vaccinations, ocdv.total_boosters,
    ocdd.new_deaths, ocdd.new_cases, ocdv.new_vaccinations, SUM(CAST(ocdv.new_vaccinations as int)) OVER (PARTITION BY ocdv.location ORDER BY ocdv.location, ocdv.date) as rolling_people_vaccinated
FROM owid_covid_data_vacc$ as ocdv
    JOIN owid_covid_data_deaths$ ocdd
    ON ocdv.[date] = ocdd.[date] AND ocdv.[location] = ocdd.[location]
WHERE ocdv.people_vaccinated IS NOT NULL
ORDER BY ocdv.[location], ocdv.[date]

--USE CTE

WITH
    PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
    as
    (
        SELECT ocdv.continent, ocdv.[location], ocdv.[date], ocdd.population, ocdv.new_vaccinations,
            SUM(CAST(ocdv.new_vaccinations as int)) OVER (PARTITION BY ocdv.location ORDER BY ocdv.location, ocdv.date) as RollingPeopleVaccinated
        FROM owid_covid_data_vacc$ as ocdv
            JOIN owid_covid_data_deaths$ ocdd
            ON ocdv.[date] = ocdd.[date] AND ocdv.[location] = ocdd.[location]
        WHERE ocdv.people_vaccinated IS NOT NULL
        --ORDER BY ocdv.[location], ocdv.[date]
    )
SELECT *, (RollingPeopleVaccinated/Population) * 100
FROM PopvsVac

--Delete tables
DROP TABLE newest_continent_data
DROP TABLE ..#newest_owid_data