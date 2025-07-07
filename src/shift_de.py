import requests
import csv
from datetime import datetime
import pytz
import argparse
import psycopg2
from psycopg2.extras import execute_values


def connect_to_db():
    """Подключение к базе данных"""
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="weather_database",
        user="weather_user",
        password="weather_pass"
    )
    return connection


def convert_to_celsius(temp_fahrenheit):
    """Конвертация градусов Фаренгейта в Цельсия"""
    return round((temp_fahrenheit - 32) * 5 / 9, 2)


def convert_knot_to_ms(speed_knots):
    """Конвертация узлов в м/с"""
    return round(float(speed_knots) * 0.514444, 2)


def unix_to_iso_with_timezone(unix_timestamp, timezone_str):
    """Преобразуем Unix-время в строку ISO 8601 с учетом часового пояса"""
    try:
        # Создаем объект datetime, представляющий время в UTC
        utc_datetime = datetime.utcfromtimestamp(unix_timestamp)
        # Получаем объект часового пояса
        timezone = pytz.timezone(timezone_str)
        # Применяем часовой пояс
        localized_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(timezone)
        # Форматируем в ISO 8601 с учетом часового пояса
        return localized_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
    except (ValueError, pytz.exceptions.UnknownTimeZoneError) as e:
        print(f"Ошибка: {e}")
        return None


def extract_data(latitude, longitude, start_date, end_date):
    """Получение данных по API в формате JSON на основе координат города и интервала дат"""
    base_url = 'https://api.open-meteo.com/v1/forecast'
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'daily': 'sunrise,sunset,daylight_duration',
        'hourly': 'temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,' +
                  'temperature_80m,temperature_120m,wind_speed_10m,wind_speed_80m,' +
                  'wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,' +
                  'weather_code,soil_temperature_0cm,soil_temperature_6cm,rain,showers,snowfall',
        'timezone': 'auto',
        'timeformat': 'unixtime',  # Используем Unix-время
        'wind_speed_unit': 'kn',
        'temperature_unit': 'fahrenheit',
        'precipitation_unit': 'inch',
        'start_date': start_date,
        'end_date': end_date
    }

    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception(f"Ошибка запроса {response.status_code}: {response.text}")
    return response.json()


def calculate_avg_daily_metrics(hourly_data, hours_in_current_day, daylight_hours=None):
    """Вычисление дневных показателей: средних и суммарных. Принимаем два набора индексов:
    hours_in_current_day - для полного дня, daylight_hours - для светлого времени суток"""
    def avg_value(key, indices):
        values = [hourly_data[key][index] for index in indices]
        total_sum = sum(values)
        count = len(values)
        return round(total_sum / count, 2) if count > 0 else None

    def total_value(key, indices):
        values = [hourly_data[key][index] for index in indices]
        return sum(values)

    # Объявляем словарь, в который складываем сначала измерения за полные сутки
    result = {}
    result.update({
        f"avg_{key}_24h": avg_value(key, hours_in_current_day)
        for key in ["temperature_2m", "relative_humidity_2m", "dew_point_2m",
                    "apparent_temperature", "temperature_80m", "temperature_120m",
                    "wind_speed_10m", "wind_speed_80m", "visibility"]
    })

    result.update({
        f"total_{key}_24h": total_value(key, hours_in_current_day)
        for key in ["rain", "showers", "snowfall"]
    })

    # Дополнительно добавляем данные для светлого времени суток
    if daylight_hours is not None:
        result.update({
            f"avg_{key}_daylight": avg_value(key, daylight_hours)
            for key in ["temperature_2m", "relative_humidity_2m", "dew_point_2m",
                        "apparent_temperature", "temperature_80m", "temperature_120m",
                        "wind_speed_10m", "wind_speed_80m", "visibility"]
        })

        result.update({
            f"total_{key}_daylight": total_value(key, daylight_hours)
            for key in ["rain", "showers", "snowfall"]
        })

    return result


def transform_data(data):
    """Функция преобразования данных"""
    # Объявляем список, который наполним приведенными к нужному виду данными
    transformed_data = []
    # Почасовые и дневные данные
    hourly_values = data['hourly']
    daily_values = data['daily']

    for day_idx in range(len(daily_values['time'])):
        # Получаем дату из Unix времени
        date_str = unix_to_iso_with_timezone(daily_values['time'][day_idx], data['timezone']).split('T')[0]
        sunrise_time = int(daily_values['sunrise'][day_idx])  # Время рассвета в Unix time
        sunset_time = int(daily_values['sunset'][day_idx])  # Время заката в Unix time
        date_start = int(daily_values['time'][day_idx])  # Время начала дня в Unix time

        # Часы, относящиеся к конкретным полным суткам
        hours_in_current_day = [
            idx for idx, timestamp in enumerate(hourly_values['time'])
            if date_start <= int(timestamp) < date_start + 86400
        ]

        # Часы, относящиеся к светлому времени суток
        daylight_hours = [
            idx for idx, timestamp in enumerate(hourly_values['time'])
            if sunrise_time <= int(timestamp) < sunset_time
        ]

        # Вычисляем средние и суммарные значения для обоих случаев
        metrics = calculate_avg_daily_metrics(
            hourly_values,
            hours_in_current_day,
            daylight_hours
        )

        # Создаем массивы с исходными данными по дням, но в других единицах измерения
        wind_speed_10m_m_per_s = \
            [convert_knot_to_ms(hourly_values['wind_speed_10m'][idx]) for idx in hours_in_current_day]
        wind_speed_80m_m_per_s = \
            [convert_knot_to_ms(hourly_values['wind_speed_80m'][idx]) for idx in hours_in_current_day]
        temperature_2m_celsius = \
            [convert_to_celsius(hourly_values['temperature_2m'][idx]) for idx in hours_in_current_day]
        apparent_temperature_celsius = \
            [convert_to_celsius(hourly_values['apparent_temperature'][idx]) for idx in hours_in_current_day]
        temperature_80m_celsius = \
            [convert_to_celsius(hourly_values['temperature_80m'][idx]) for idx in hours_in_current_day]
        temperature_120m_celsius = \
            [convert_to_celsius(hourly_values['temperature_120m'][idx]) for idx in hours_in_current_day]
        soil_temperature_0cm_celsius = \
            [convert_to_celsius(hourly_values['soil_temperature_0cm'][idx]) for idx in hours_in_current_day]
        soil_temperature_6cm_celsius = \
            [convert_to_celsius(hourly_values['soil_temperature_6cm'][idx]) for idx in hours_in_current_day]
        rain_mm = [25.4 * hourly_values['rain'][idx] for idx in hours_in_current_day]       # Перевод дюймов в мм
        showers_mm = [25.4 * hourly_values['showers'][idx] for idx in hours_in_current_day]
        snowfall_mm = [25.4 * hourly_values['snowfall'][idx] for idx in hours_in_current_day]

        # Создаем словарь с итоговыми данными
        record = {
            'date': date_str,

            # Значения для полных суток
            'avg_temperature_2m_24h': convert_to_celsius(metrics['avg_temperature_2m_24h']),
            'avg_relative_humidity_2m_24h': metrics['avg_relative_humidity_2m_24h'],
            'avg_dew_point_2m_24h': convert_to_celsius(metrics['avg_dew_point_2m_24h']),
            'avg_apparent_temperature_24h': convert_to_celsius(metrics['avg_apparent_temperature_24h']),
            'avg_temperature_80m_24h': convert_to_celsius(metrics['avg_temperature_80m_24h']),
            'avg_temperature_120m_24h': convert_to_celsius(metrics['avg_temperature_120m_24h']),
            'avg_wind_speed_10m_24h': convert_knot_to_ms(metrics['avg_wind_speed_10m_24h']),
            'avg_wind_speed_80m_24h': convert_knot_to_ms(metrics['avg_wind_speed_80m_24h']),
            'avg_visibility_24h': metrics['avg_visibility_24h'] * 0.3048,       # Перевод футов в м
            'total_rain_24h': metrics['total_rain_24h'] * 25.4,                 # Перевод дюймов в мм
            'total_showers_24h': metrics['total_showers_24h'] * 25.4,
            'total_snowfall_24h': metrics['total_snowfall_24h'] * 25.4,

            # Значения для светлого времени суток
            'avg_temperature_2m_daylight': convert_to_celsius(metrics['avg_temperature_2m_daylight']),
            'avg_relative_humidity_2m_daylight': metrics['avg_relative_humidity_2m_daylight'],
            'avg_dew_point_2m_daylight': convert_to_celsius(metrics['avg_dew_point_2m_daylight']),
            'avg_apparent_temperature_daylight': convert_to_celsius(metrics['avg_apparent_temperature_daylight']),
            'avg_temperature_80m_daylight': convert_to_celsius(metrics['avg_temperature_80m_daylight']),
            'avg_temperature_120m_daylight': convert_to_celsius(metrics['avg_temperature_120m_daylight']),
            'avg_wind_speed_10m_daylight': convert_knot_to_ms(metrics['avg_wind_speed_10m_daylight']),
            'avg_wind_speed_80m_daylight': convert_knot_to_ms(metrics['avg_wind_speed_80m_daylight']),
            'avg_visibility_daylight': metrics['avg_visibility_daylight'] * 0.3048,
            'total_rain_daylight': metrics['total_rain_daylight'] * 25.4,
            'total_showers_daylight': metrics['total_showers_daylight'] * 25.4,
            'total_snowfall_daylight': metrics['total_snowfall_daylight'] * 25.4,

            # Массивы данных за день в нужных единицах измерения
            'wind_speed_10m_m_per_s': wind_speed_10m_m_per_s,  # Массив скоростей ветра на высоте 10 м
            'wind_speed_80m_m_per_s': wind_speed_80m_m_per_s,  # Массив скоростей ветра на высоте 80 м
            'temperature_2m_celsius': temperature_2m_celsius,  # Массив температур воздуха
            'apparent_temperature_celsius': apparent_temperature_celsius,
            'temperature_80m_celsius': temperature_80m_celsius,
            'temperature_120m_celsius': temperature_120m_celsius,
            'soil_temperature_0cm_celsius': soil_temperature_0cm_celsius,
            'soil_temperature_6cm_celsius': soil_temperature_6cm_celsius,
            'rain_mm': rain_mm,
            'showers_mm': showers_mm,
            'snowfall_mm': snowfall_mm,

            'daylight_hours': (sunset_time - sunrise_time) / 3600,  # Разница в секундах делится на 3600 для часов
            'sunset_iso': unix_to_iso_with_timezone(sunset_time, data['timezone']),
            'sunrise_iso': unix_to_iso_with_timezone(sunrise_time, data['timezone'])
        }

        # Дополняем список данными по каждому дню
        transformed_data.append(record)

    return transformed_data


def save_data(transformed_data, filename='weather_report.csv'):
    """Записываем данные в файл"""
    field_names = list(transformed_data[0].keys())
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(transformed_data)
    print(f"Данные успешно записаны в файл '{filename}'")


def load_data(weather_data):
    """Загружаем данные в БД.
    Борьба с дубликатами реализована следующим образом:
    благодаря конструкции "ON CONFLICT (date) DO UPDATE SET ... EXCLUDED ..."
    при попытке вставить в БД данные за день (date), который уже содержится в ней,
    происходит не вставка еще одной строки-дубля, а обновление всех полей,
    относящихся к этому дню, вновь вставляемыми данными"""
    conn = connect_to_db()
    cursor = conn.cursor()

    # Определение всех необходимых столбцов таблицы
    columns = ", ".join(weather_data[0].keys())

    # SQL-шаблон для массовых вставок
    sql_template = f"""INSERT INTO weather_schema.weather_records ({columns}) VALUES %s 
    ON CONFLICT (date) 
    DO UPDATE SET avg_temperature_2m_24h = EXCLUDED.avg_temperature_2m_24h, 
    avg_relative_humidity_2m_24h = EXCLUDED.avg_relative_humidity_2m_24h, 
    avg_dew_point_2m_24h = EXCLUDED.avg_dew_point_2m_24h, 
    avg_apparent_temperature_24h = EXCLUDED.avg_apparent_temperature_24h, 
    avg_temperature_80m_24h = EXCLUDED.avg_temperature_80m_24h, 
    avg_temperature_120m_24h = EXCLUDED.avg_temperature_120m_24h, 
    avg_wind_speed_10m_24h = EXCLUDED.avg_wind_speed_10m_24h, 
    avg_wind_speed_80m_24h = EXCLUDED.avg_wind_speed_80m_24h, 
    avg_visibility_24h = EXCLUDED.avg_visibility_24h, total_rain_24h = EXCLUDED.total_rain_24h, 
    total_showers_24h = EXCLUDED.total_showers_24h, total_snowfall_24h = EXCLUDED.total_snowfall_24h, 
    
    avg_temperature_2m_daylight = EXCLUDED.avg_temperature_2m_daylight, 
    avg_relative_humidity_2m_daylight = EXCLUDED.avg_relative_humidity_2m_daylight, 
    avg_dew_point_2m_daylight = EXCLUDED.avg_dew_point_2m_daylight, 
    avg_apparent_temperature_daylight = EXCLUDED.avg_apparent_temperature_daylight, 
    avg_temperature_80m_daylight = EXCLUDED.avg_temperature_80m_daylight, 
    avg_temperature_120m_daylight = EXCLUDED.avg_temperature_120m_daylight, 
    avg_wind_speed_10m_daylight = EXCLUDED.avg_wind_speed_10m_daylight, 
    avg_wind_speed_80m_daylight = EXCLUDED.avg_wind_speed_80m_daylight, 
    avg_visibility_daylight = EXCLUDED.avg_visibility_daylight, total_rain_daylight = EXCLUDED.total_rain_daylight, 
    total_showers_daylight = EXCLUDED.total_showers_daylight, 
    total_snowfall_daylight = EXCLUDED.total_snowfall_daylight, 
    
    wind_speed_10m_m_per_s = EXCLUDED.wind_speed_10m_m_per_s, 
    wind_speed_80m_m_per_s = EXCLUDED.wind_speed_80m_m_per_s, 
    temperature_2m_celsius = EXCLUDED.temperature_2m_celsius, 
    apparent_temperature_celsius = EXCLUDED.apparent_temperature_celsius, 
    temperature_80m_celsius = EXCLUDED.temperature_80m_celsius, 
    temperature_120m_celsius = EXCLUDED.temperature_120m_celsius, 
    soil_temperature_0cm_celsius = EXCLUDED.soil_temperature_0cm_celsius, 
    soil_temperature_6cm_celsius = EXCLUDED.soil_temperature_6cm_celsius, 
    rain_mm = EXCLUDED.rain_mm, 
    showers_mm = EXCLUDED.showers_mm, 
    snowfall_mm = EXCLUDED.snowfall_mm, 
    
    daylight_hours = EXCLUDED.daylight_hours, sunset_iso = EXCLUDED.sunset_iso, 
    sunrise_iso = EXCLUDED.sunrise_iso
    ; """

    try:
        # Передача кортежей значений
        records_list = [tuple(d.values()) for d in weather_data]

        # Выполняем массовую вставку
        execute_values(cursor, sql_template, records_list)
        conn.commit()
        print("Данные успешно вставлены в базу данных.")
    except Exception as err:
        print(f"Ошибка при вставке данных: {err}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    """Указываем аргументы, необходимые при запуске скрипта из командной строки :
    (координаты, даты, указатели на сохранение в файл или запись в БД"""
    parser = argparse.ArgumentParser(description="ETL pipeline для выгрузки прогноза погоды.")
    parser.add_argument('--lat', type=float, required=True, help="Широта местоположения")
    parser.add_argument('--lon', type=float, required=True, help="Долгота местоположения")
    parser.add_argument('--start-date', type=str, required=True, help="Начальная дата (формат YYYY-MM-DD)")
    parser.add_argument('--end-date', type=str, required=True, help="Конечная дата (формат YYYY-MM-DD)")
    parser.add_argument('--load-only', action='store_true', help="Только загрузить данные в БД")
    parser.add_argument('--save-only', action='store_true', help="Только сохранить данные в CSV")
    parser.add_argument('--both', action='store_true', help="Выполнить обе операции (по умолчанию)")

    args = parser.parse_args()

    try:
        # извлекаем данные
        raw_data = extract_data(args.lat, args.lon, args.start_date, args.end_date)
        # преобразуем
        processed_data = transform_data(raw_data)

        # сохраняем и/или загружаем
        if args.save_only:
            save_data(processed_data)
        elif args.load_only:
            load_data(processed_data)
        else:
            # По умолчанию выполняем обе операции
            save_data(processed_data)
            load_data(processed_data)

    except Exception as e:
        print("Возникла ошибка:", str(e))

# python src/shift_de.py --lat 55.0344 --lon 82.9434 --start-date 2025-05-16 --end-date 2025-05-30
