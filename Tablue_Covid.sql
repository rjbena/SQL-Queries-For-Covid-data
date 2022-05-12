-- Queries for Tableau


-- Summary of World death data
SELECT SUM(cast(new_cases as int)) as total_cases, SUM(cast( new_deaths as int)) as total_deaths, SUM(cast( new_deaths as float))/SUM(cast(new_cases as float)) * 100 as death_percentage
FROM owid_covid_data_deaths$

-- Summary of location death data
SELECT [location], SUM(cast(new_deaths as bigint)) as total_death_count, SUM(Cast(new_cases as bigint)) as total_cases
FROM owid_covid_data_deaths$
GROUP BY [location]
ORDER BY total_death_count DESC

-- Max deaths
SELECT [location], MAX(total_deaths) as total_deaths
FROM owid_covid_data_deaths$
GROUP BY [location]
ORDER BY total_deaths DESC
-- Percent of population infected

SELECT [location], population, MAX(total_cases) as highest_infection_count, MAX((total_cases/population)) * 100 as percent_population_infected
FROM owid_covid_data_deaths$

-- dates not in both tables.
SELECT [date]
FROM owid_covid_data_vacc$
WHERE date NOT IN (
    Select date
from owid_covid_data_deaths$
)