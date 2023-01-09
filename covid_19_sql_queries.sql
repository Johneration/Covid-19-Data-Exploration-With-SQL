-- Covid-19 Data Exploration 
-- Skills used: Creating Tables, Inserting Data, Aggregate Functions, Joins, Subquery


-- Create tables
CREATE TABLE CovidDeath (
    continent VARCHAR(50),
    location VARCHAR(50), # country
    date_create DATE,
    population INT,
    total_cases INT,
    new_cases INT,
    total_deaths INT,
    new_deaths INT,
    total_cases_per_million FLOAT,
    new_cases_per_million FLOAT
);

CREATE TABLE CovidVaccination (
    continent VARCHAR(50),
    location VARCHAR(50),
    date_create DATE,
    total_vaccinations INT,
    people_vaccinated INT,
    people_fully_vaccinated INT,
    total_boosters INT,
    new_vaccinations INT,
    total_vaccinations_per_hundred FLOAT,
    people_vaccinated_per_hundred FLOAT,
    people_fully_vaccinated_per_hundred FLOAT,
    total_boosters_per_hundred FLOAT
);

-- Insert data from CSV files into the tables we just created
LOAD DATA INFILE '/usr/local/mysql-8.0.31-macos12-arm64/portfolio_project/CovidDeath.csv' 
INTO TABLE CovidDeath 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE '/usr/local/mysql-8.0.31-macos12-arm64/portfolio_project/CovidVaccination.csv' 
INTO TABLE CovidVaccination 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Examine the total number of cases versus the total number of deaths in the US
SELECT
    location,
    date_create,
    total_cases,
    total_deaths,
    ROUND((total_deaths/total_cases)*100,3) AS DeathRate
FROM CovidDeath
WHERE 
    location LIKE '%states';

-- Examine the top 20 countries with the highest COVID-19 infection rate as of October 30, 2022 
-- (Only consider countries with a population over 5,000,000)
SELECT
    continent,
    location,
    date_create,
    population,
    total_cases,
    ROUND((total_cases/population)*100,3) AS CovidPercentage
FROM CovidDeath
WHERE
    date_create = '2022-10-30' AND
    population > 5000000 AND
    continent != ''
ORDER BY CovidPercentage DESC
LIMIT 20;

-- Examine the top 20 countries with the highest COVID-19 death rate
-- (Only consider countries with a population over 5,000,000)
SELECT
    continent,
    location,
    population,
    MAX(total_deaths),
    MAX(ROUND((total_deaths/population)*100,3)) AS DeahtPercentage
FROM CovidDeath
WHERE 
    population > 5000000 AND
    continent != ''
GROUP BY 1, 2, 3
ORDER BY DeahtPercentage DESC
LIMIT 20;

-- Break down the data by continent and check the statistics as of October 30, 2022
SELECT
    continent,
    SUM(population) AS population,
    SUM(total_cases) AS total_case,
    SUM(total_deaths) AS total_deaths,
    ROUND(SUM(total_cases)/SUM(population)*100,3) AS CovidPercentage,
    ROUND(SUM(total_deaths)/SUM(population)*100,3) AS DeathsPercentage
FROM CovidDeath
WHERE
    continent != '' AND
    date_create = '2022-10-30'
GROUP BY continent;

-- Demonstrate two ways of calculating rolling vaccinations and check if the total vaccination numbers reported match the calculated ones
SELECT
    a.location,
    a.date_create,
    a.new_vaccinations,
    # Use 'OVER PARTITION BY' to create a sum of daily new vaccinations
    SUM(a.new_vaccinations) OVER (PARTITION BY a.location ORDER BY a.date_create) AS rolling_vaccinations_partition,
    # Use a subquery to create a sum of daily new vaccinations
    (
        SELECT SUM(b.new_vaccinations)
        FROM CovidVaccination b
        WHERE a.location = b.location AND b.date_create <= a.date_create
    ) AS rolling_vaccinations_subquery,
    # The actual total vaccinations reported by the organization
    total_vaccinations AS total_vaccination_repoted
FROM CovidVaccination a
WHERE location = 'United States';


-- Join the death and vaccination table for further analysis
SELECT
    d.continent,
    d.location,
    d.new_cases,
    d.total_cases,
    d.new_deaths,
    d.total_deaths,
    v.new_vaccinations,
    SUM(v.new_vaccinations) OVER (PARTITION BY v.location ORDER BY v.date_create) AS rolling_vaccinations,
    v.total_vaccinations,
    v.people_vaccinated
FROM CovidDeath d
    JOIN CovidVaccination v USING(location, date_create)
WHERE d.continent != '';