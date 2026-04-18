-- This SQL script is read by test.php on ?initSQL=1
-- It drops existing tables and creates fresh ones.

DROP TABLE IF EXISTS countries;
DROP TABLE IF EXISTS continents;

CREATE TABLE continents (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    area_km2 BIGINT,
    population BIGINT
);

INSERT INTO continents (name, area_km2, population) VALUES
('Africa', 30370000, 1340000000),
('Antarctica', 14000000, 0),
('Asia', 44579000, 4640000000),
('Europe', 10180000, 746000000),
('North America', 24709000, 592000000),
('Oceania', 8526000, 43000000),
('South America', 17840000, 430000000);

CREATE TABLE countries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    continent_id INT NOT NULL,
    population BIGINT,
    area_km2 BIGINT,
    capital VARCHAR(100),
    founded_date DATE,
    FOREIGN KEY (continent_id) REFERENCES continents(id) ON DELETE CASCADE
);

-- Insert countries (Turkey and Azerbaijan first) with realistic founded_date
INSERT INTO countries (name, continent_id, population, area_km2, capital, founded_date) VALUES
('Turkey', 3, 84300000, 783600, 'Ankara', '1923-10-29'),
('Azerbaijan', 3, 10100000, 86600, 'Baku', '1991-10-18'),
('China', 3, 1412000000, 9597000, 'Beijing', '-1600-01-01'),
('India', 3, 1380000000, 3287000, 'New Delhi', '1947-08-15'),
('Indonesia', 3, 273000000, 1905000, 'Jakarta', '1945-08-17'),
('Pakistan', 3, 220000000, 881900, 'Islamabad', '1947-08-14'),
('Bangladesh', 3, 166000000, 147600, 'Dhaka', '1971-03-26'),
('Japan', 3, 125800000, 378000, 'Tokyo', '-0660-02-11'),
('Saudi Arabia', 3, 34800000, 2150000, 'Riyadh', '1932-09-23'),
('Kazakhstan', 3, 18700000, 2724900, 'Nur-Sultan', '1991-12-16'),
('Nigeria', 1, 206000000, 923800, 'Abuja', '1960-10-01'),
('Ethiopia', 1, 114000000, 1104000, 'Addis Ababa', NULL),
('Egypt', 1, 102000000, 1001000, 'Cairo', NULL),
('DR Congo', 1, 89500000, 2345000, 'Kinshasa', '1960-06-30'),
('South Africa', 1, 59300000, 1221000, 'Pretoria', '1910-05-31'),
('Russia', 4, 146000000, 17098200, 'Moscow', '0862-01-01'),
('Germany', 4, 83000000, 357000, 'Berlin', '1990-10-03'),
('France', 4, 67000000, 551000, 'Paris', '0843-08-01'),
('United Kingdom', 4, 67000000, 243600, 'London', '0927-01-01'),
('Italy', 4, 60300000, 301300, 'Rome', '-0753-04-21'),
('Sweden', 4, 10300000, 450300, 'Stockholm', '1523-06-06'),
('Iceland', 4, 364000, 103000, 'Reykjavik', '0930-01-01'),
('USA', 5, 331000000, 9834000, 'Washington D.C.', '1776-07-04'),
('Canada', 5, 38000000, 9985000, 'Ottawa', '1867-07-01'),
('Mexico', 5, 128000000, 1964000, 'Mexico City', '1821-09-27'),
('Greenland', 5, 56000, 2166000, 'Nuuk', '1979-05-01'),
('Brazil', 7, 213000000, 8516000, 'Brasília', '1822-09-07'),
('Argentina', 7, 45000000, 2780000, 'Buenos Aires', '1816-07-09'),
('Chile', 7, 19100000, 756100, 'Santiago', '1818-02-12'),
('Uruguay', 7, 3470000, 176200, 'Montevideo', '1825-08-25'),
('Australia', 6, 25700000, 7692000, 'Canberra', '1901-01-01'),
('New Zealand', 6, 5100000, 268000, 'Wellington', '1907-09-26'),
('Papua New Guinea', 6, 8900000, 462800, 'Port Moresby', '1975-09-16');

CREATE INDEX idx_countries_continent ON countries(continent_id);