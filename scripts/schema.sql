CREATE SCHEMA IF NOT EXISTS weather_schema AUTHORIZATION weather_user;
SET search_path TO weather_schema;

CREATE TABLE IF NOT EXISTS weather_records (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,

    -- Средние показатели за весь день
    avg_temperature_2m_24h FLOAT,
    avg_relative_humidity_2m_24h FLOAT,
    avg_dew_point_2m_24h FLOAT,
    avg_apparent_temperature_24h FLOAT,
    avg_temperature_80m_24h FLOAT,
    avg_temperature_120m_24h FLOAT,
    avg_wind_speed_10m_24h FLOAT,
    avg_wind_speed_80m_24h FLOAT,
    avg_visibility_24h FLOAT,
    total_rain_24h FLOAT,
    total_showers_24h FLOAT,
    total_snowfall_24h FLOAT,

    -- Средние показатели за дневное время
    avg_temperature_2m_daylight FLOAT,
    avg_relative_humidity_2m_daylight FLOAT,
    avg_dew_point_2m_daylight FLOAT,
    avg_apparent_temperature_daylight FLOAT,
    avg_temperature_80m_daylight FLOAT,
    avg_temperature_120m_daylight FLOAT,
    avg_wind_speed_10m_daylight FLOAT,
    avg_wind_speed_80m_daylight FLOAT,
    avg_visibility_daylight FLOAT,
    total_rain_daylight FLOAT,
    total_showers_daylight FLOAT,
    total_snowfall_daylight FLOAT,

    -- Массивы данных (не рекомендуется хранить массивы в реляционной БД, лучше сохранить агрегированные значения)
    -- Если всё-таки нужны массивы, используйте тип JSONB или ARRAY
    wind_speed_10m_m_per_s NUMERIC[],
    wind_speed_80m_m_per_s NUMERIC[],
    temperature_2m_celsius NUMERIC[],
    apparent_temperature_celsius NUMERIC[],
    temperature_80m_celsius NUMERIC[],
    temperature_120m_celsius NUMERIC[],
    soil_temperature_0cm_celsius NUMERIC[],
    soil_temperature_6cm_celsius NUMERIC[],
    rain_mm NUMERIC[],
    showers_mm NUMERIC[],
    snowfall_mm NUMERIC[],

    -- Дополнительные характеристики
    daylight_hours FLOAT,
    sunset_iso TIMESTAMPTZ,
    sunrise_iso TIMESTAMPTZ,

    CONSTRAINT unique_date UNIQUE (date)
);