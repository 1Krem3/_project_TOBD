"""
ETL Flow для обработки данных о выбросах в атмосферу
Использует Prefect для оркестрации и Dask для распределенной обработки
"""

import sqlite3
from datetime import datetime
from typing import Tuple, Optional
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect_dask import DaskTaskRunner
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

db_path = "/home/user/Desktop/air-quality-project/data/air_emissions.db"
csv_path = "/home/user/Desktop/air-quality-project/data/air_emissions.csv"

# ============================================================================
# TASKS (Задачи)
# ============================================================================

@task(name="extract_data", retries=2, retry_delay_seconds=30)
def extract_data(file_path: str = csv_path) -> dd.DataFrame:
    """
    Задача извлечения данных из CSV файла
    """
    logger = get_run_logger()
    logger.info(f"Начало загрузки данных из {file_path}")
    
    try:
        # Загружаем все как строки для безопасного парсинга
        df = dd.read_csv(
            file_path,
            sep=';',
            encoding='utf-8',
            dtype=str,  # Все как строки
            on_bad_lines='skip',
            assume_missing=True
        )
        
        logger.info(f"Данные загружены. Столбцов: {len(df.columns)}, предпросмотр: {len(df):,} строк")
        logger.info(f"Столбцы: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        raise

@task(name="transform_data")
def transform_data(df: dd.DataFrame) -> pd.DataFrame:
    """
    Задача преобразования и очистки данных
    """
    logger = get_run_logger()
    logger.info("Начало трансформации данных")
    
    try:
        # Шаг 1: Переименование столбцов
        new_names = [
            'section', 'indicator', 'unit', 'code', 'substance',
            'source_type', 'emission_type', 'location_level', 'region',
            'municipal_district', 'municipal_formation', 'oktmo_code',
            'year', 'value'
        ]
        df = df.rename(columns=dict(zip(df.columns, new_names)))
        logger.info("Столбцы переименованы")
        
        # Шаг 2: Очистка от пропусков
        logger.info("Удаление пропусков...")
        df_cleaned = df.dropna(subset=['value', 'section', 'code', 'substance'])
        
        # Шаг 3: Преобразование типов данных
        logger.info("Преобразование типов данных...")
        if 'year' in df_cleaned.columns:
            df_cleaned['year'] = dd.to_numeric(df_cleaned['year'], errors='coerce')
        if 'value' in df_cleaned.columns:
            df_cleaned['value'] = df_cleaned['value'].str.replace(',', '.').astype('float')
        
        # Шаг 4: Вычисляем результат (переход от Dask к Pandas)
        logger.info("Вычисление финального DataFrame...")
        df_final = df_cleaned.compute()
        
        # Шаг 5: Фильтрация данных
        logger.info(f"Исходное количество записей: {len(df_final):,}")
        logger.info("Применение фильтров...")
        
        # Удаляем строки с некорректными значениями
        df_final = df_final[df_final['value'] != 9999999999.0]
        
        # Удаляем некорректные вещества
        df_final = df_final[~df_final['substance'].isin(['CD', 'ND'])]
        
        logger.info(f"Количество записей после фильтрации: {len(df_final):,}")
        logger.info(f"Годы в данных: от {df_final['year'].min()} до {df_final['year'].max()}")
        logger.info(f"Уникальных веществ: {df_final['substance'].nunique()}")
        
        return df_final
        
    except Exception as e:
        logger.error(f"Ошибка при трансформации данных: {e}")
        raise

@task(name="create_database_tables")
def create_database_tables(
    df_final: pd.DataFrame, 
    db_path: str = db_path
) -> Tuple[int, dict]:
    """
    Задача создания таблиц в базе данных
    Возвращает статистику по созданным таблицам
    """
    logger = get_run_logger()
    logger.info(f"Создание базы данных: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        stats = {}
        
        # ============================================================================
        # 1. Основная таблица air_emissions
        # ============================================================================
        logger.info("Создание основной таблицы air_emissions...")
        air_emissions_cols = ['section', 'code', 'substance', 'value', 'oktmo_code', 'year']
        air_emissions_df = df_final[air_emissions_cols].copy()
        air_emissions_df.to_sql('air_emissions', conn, if_exists='replace', index=False)
        stats['air_emissions'] = len(air_emissions_df)
        logger.info(f"Таблица 'air_emissions': {len(air_emissions_df):,} записей")
        
        # ============================================================================
        # 2. Таблица indicator_codes
        # ============================================================================
        logger.info("Создание таблицы indicator_codes...")
        indicator_data = df_final[['code', 'indicator']].drop_duplicates()
        indicator_data = indicator_data.sort_values('code')
        indicator_data.to_sql('indicator_codes', conn, if_exists='replace', index=False)
        stats['indicator_codes'] = len(indicator_data)
        logger.info(f"Таблица 'indicator_codes': {len(indicator_data):,} записей")
        
        # ============================================================================
        # 3. Таблица substance_types
        # ============================================================================
        logger.info("Создание таблицы substance_types...")
        substance_data = df_final[['substance', 'source_type']].drop_duplicates('substance')
        substance_data = substance_data.sort_values('substance')
        substance_data.to_sql('substance_types', conn, if_exists='replace', index=False)
        stats['substance_types'] = len(substance_data)
        logger.info(f"Таблица 'substance_types': {len(substance_data):,} записей")
        
        # ============================================================================
        # 4. Таблица location_codes
        # ============================================================================
        logger.info("Создание таблицы location_codes...")
        location_cols = ['oktmo_code', 'municipal_formation', 'municipal_district', 'region']
        location_data = df_final[location_cols].drop_duplicates()
        location_data = location_data.sort_values('oktmo_code')
        location_data.to_sql('location_codes', conn, if_exists='replace', index=False)
        stats['location_codes'] = len(location_data)
        logger.info(f"Таблица 'location_codes': {len(location_data):,} записей")
        
        # ============================================================================
        # Создание индексов
        # ============================================================================
        logger.info("Создание индексов...")
        indexes = [
            ("air_emissions", "idx_air_year", "year"),
            ("air_emissions", "idx_air_code", "code"),
            ("air_emissions", "idx_air_substance", "substance"),
            ("air_emissions", "idx_air_section", "section"),
            ("air_emissions", "idx_air_oktmo", "oktmo_code"),
            ("indicator_codes", "idx_indicator_code", "code"),
            ("substance_types", "idx_substance", "substance"),
            ("location_codes", "idx_location_oktmo", "oktmo_code"),
        ]
        
        for table, idx_name, column in indexes:
            try:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column});")
                logger.debug(f"Индекс {idx_name} создан")
            except Exception as e:
                logger.warning(f"Ошибка при создании индекса {idx_name}: {e}")
        
        conn.commit()
        conn.close()
        
        total_records = sum(stats.values())
        logger.info(f"Всего сохранено записей: {total_records:,}")
        
        return total_records, stats
        
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц: {e}")
        raise

@task(name="validate_database")
def validate_database(db_path: str = db_path) -> dict:
    """
    Задача валидации базы данных
    Проверяет корректность созданных таблиц
    """
    logger = get_run_logger()
    logger.info(f"Валидация базы данных: {db_path}")
    
    try:
        validation_results = {}
        
        # Получаем список всех таблиц
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = cursor.fetchall()
        
        logger.info(f"Найдено таблиц: {len(tables)}")
        
        for table in tables:
            table_name = table[0]
            
            # Количество записей
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            
            # Столбцы
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            # Пример данных (первые 2 строки)
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 2;")
            sample = cursor.fetchall()
            
            validation_results[table_name] = {
                'row_count': count,
                'columns': column_names,
                'sample': sample
            }
            
            logger.info(f"📊 {table_name}: {count:,} записей, столбцы: {', '.join(column_names[:3])}...")
            
            if count == 0:
                logger.warning(f"Таблица {table_name} пустая!")
        
        # Проверка связей между таблицами
        logger.info("Проверка связей между таблицами...")
        
        checks = [
            ("Проверка indicator_codes", 
             "SELECT COUNT(DISTINCT code) FROM air_emissions WHERE code NOT IN (SELECT code FROM indicator_codes)"),
            ("Проверка substance_types",
             "SELECT COUNT(DISTINCT substance) FROM air_emissions WHERE substance NOT IN (SELECT substance FROM substance_types)"),
            ("Проверка location_codes",
             "SELECT COUNT(DISTINCT oktmo_code) FROM air_emissions WHERE oktmo_code NOT IN (SELECT oktmo_code FROM location_codes)")
        ]
        
        for check_name, query in checks:
            try:
                cursor.execute(query)
                missing = cursor.fetchone()[0]
                if missing == 0:
                    logger.info(f"{check_name}: все ссылки корректны")
                else:
                    logger.warning(f"{check_name}: {missing} отсутствующих ссылок")
            except Exception as e:
                logger.error(f"Ошибка при проверке {check_name}: {e}")
        
        conn.close()
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Ошибка при валидации базы данных: {e}")
        raise

# ============================================================================
# FLOW (Основной поток)
# ============================================================================

@flow(
    name="Air Quality ETL Pipeline",
    description="Полный ETL-пайплайн для обработки данных о выбросах в атмосферу",
    task_runner=DaskTaskRunner(),
    version="1.0.0",
    log_prints=True
)
def air_quality_etl_flow(
    data_file: str = csv_path,
    db_file: str = db_path,
    run_validation: bool = True
) -> dict:
    """
    Основной ETL Flow для обработки данных о качестве воздуха
    
    Args:
        data_file: Путь к исходному CSV файлу
        db_file: Путь к SQLite базе данных
        run_validation: Запускать ли валидацию базы данных
    
    Returns:
        Словарь с результатами выполнения
    """
    logger = get_run_logger()
    logger.info("Запуск Air Quality ETL Pipeline")
    logger.info(f"Входной файл: {data_file}")
    logger.info(f"Выходная БД: {db_file}")
    logger.info(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Запускаем задачи последовательно
    raw_data = extract_data(data_file)
    transformed_data = transform_data(raw_data)
    total_records, table_stats = create_database_tables(transformed_data, db_file)
    
    if run_validation:
        validation_results = validate_database(db_file)
    else:
        validation_results = {}
    
    # Сводная статистика
    summary = {
        'timestamp': datetime.now().isoformat(),
        'input_file': data_file,
        'database_file': db_file,
        'total_records_processed': total_records,
        'table_statistics': table_stats,
        'validation_results': validation_results,
        'status': 'COMPLETED'
    }
    
    logger.info("=" * 50)
    logger.info("ИТОГИ ВЫПОЛНЕНИЯ:")
    logger.info("=" * 50)
    logger.info(f"Статус: {summary['status']}")
    logger.info(f"Обработано записей: {total_records:,}")
    logger.info(f"Таблицы созданы: {', '.join(table_stats.keys())}")
    logger.info(f"Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("ETL-пайплайн успешно завершен!")
    
    return summary

# ============================================================================
# ЗАПУСК ПРИЛОЖЕНИЯ
# ============================================================================

if __name__ == "__main__":
    # Вариант 1: Прямой запуск flow
    result = air_quality_etl_flow(
        data_file=csv_path,
        db_file=db_path,
        run_validation=True
    )
    
    print("\n" + "="*60)
    print("РЕЗУЛЬТАТЫ ВЫПОЛНЕНИЯ:")
    print("="*60)
    print(f"Статус: {result['status']}")
    print(f"Всего записей: {result['total_records_processed']:,}")
    for table, count in result['table_statistics'].items():
        print(f"  {table}: {count:,} записей")