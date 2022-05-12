SELECT SUM(CAST(new_cases as int)) as total_cases, SUM(cast(new_deaths as int)) as total_deaths,
    SUM(CAST(new_deaths as float))/SUM(CAST(new_cases as float)) * 100 as DeathPercentage
FROM owid_covid_data_deaths$

SELECT continent, SUM(CAST(new_deaths as int)) as total_deaths
FROM continent_data_deaths$
GROUP BY continent
ORDER BY total_deaths DESC

SELECT [location], population, MAX(total_cases) as highest_infection_count, MAX((total_cases/population)) * 100 as percent_population_infected
FROM owid_covid_data_deaths$
GROUP BY [location],population
ORDER BY percent_population_infected DESC

SELECT [location], population, date, MAX(total_cases) as highest_infection_count, MAX((total_cases/population)) * 100 as percent_population_infected
FROM owid_covid_data_deaths$
GROUP BY [location],population,date
ORDER BY percent_population_infected DESC

SELECT dea.continent, dea.[location], dea.[date], dea.population,
    MAX(vac.total_vaccinations) as rolling_people_vaccinated
FROM owid_covid_data_deaths$ as dea
    JOIN owid_covid_data_vacc$ as vac
    ON dea.[location] = vac.[location]
        AND dea.[date] = vac.[date]
GROUP BY dea.continent, dea.[location], dea.[date], dea.population
ORDER BY 1,2,3

SELECT dea.location, dea.population, vac.median_age, vac.aged_65_older, vac.aged_70_older, SUM(cast(dea.new_cases as int )) as total_cases_count, SUM(CAST(dea.new_deaths as int)) as total_death_count, SUM(CAST(vac.new_vaccinations as bigint)) as total_vaccination_count
FROM owid_covid_data_deaths$ dea
    JOIN owid_covid_data_vacc$ vac
    ON dea.[location] = vac.[location]
        AND dea.[date] = vac.[date]
GROUP BY dea.[location], dea.population,vac.median_age, vac.aged_65_older, vac.aged_70_older
ORDER BY total_death_count DESC


WITH
    pop_vs_vac (continent, location, date, population, new_vaccinations, rolling_people_vaccinated)
    AS
    (
        SELECT dea.continent, dea.[location], dea.[date], dea.population, vac.new_vaccinations
        , SUM(cast(new_vaccinations as bigint)) OVER (Partition BY dea.location ORDER BY dea.location, dea.date) as rolling_people_vaccinated
        FROM owid_covid_data_deaths$ dea
            JOIN owid_covid_data_vacc$ vac
            ON dea.[location] = vac.[location] AND dea.[date] = vac.[date]
    )
Select *, (rolling_people_vaccinated/population) * 100 as percent_population_vaccinated
FROM pop_vs_vac
WHERE rolling_people_vaccinated < population

SELECT dea.[location], dea.population, MAX(total_cases) as total_cases, MAX(dea.total_deaths) as total_deaths, MAX(vac.total_vaccinations) as total_vaccinatiions
    , MAX(dea.total_cases)/dea.population * 100 as percent_infected, MAX(dea.total_deaths)/dea.population  * 100 as percent_of_covid_deaths, MAX(vac.total_vaccinations)/dea.population * 100 as percent_vaccinated
FROM owid_covid_data_deaths$ dea
    JOIN owid_covid_data_vacc$ vac
    ON dea.[location] = vac.[location] AND dea.[date] = vac.[date]
GROUP BY dea.[location], dea.population
HAVING dea.population > MAX(vac.total_vaccinations)
ORDER BY dea.[location]

SELECT DISTINCT([location]), population
FROM owid_covid_data_deaths$

SELECT [location], MAX(cast(total_vaccinations as bigint)) as total_vacc, SUM(cast(new_vaccinations_smoothed as bigint)) as total_vacc_smoothed
FROM owid_covid_data_vacc$
GROUP BY [location]