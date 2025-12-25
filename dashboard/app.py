"""
Streamlit Dashboard для анализа выбросов в атмосферу
Визуализация данных из ETL пайплайна
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Настройки страницы
st.set_page_config(
    page_title="Air Quality Analytics",
    page_icon="🇷🇺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Глобальная переменная для пути к БД
db_path = "data/air_emissions.db"
csv_path = "data/air_emissions.csv"

# Функция для создания нового соединения в каждом потоке
def get_connection():
    """Создает новое соединение с БД в текущем потоке"""
    return sqlite3.connect(db_path, check_same_thread=False)

# Функция загрузки данных с правильными джойнами
@st.cache_data(ttl=3600)
def load_data():
    """Загружает данные из базы"""
    conn = get_connection()
    
    try:
        # Получаем все вещества с их типами
        query_substances = """
        SELECT DISTINCT substance, source_type 
        FROM substance_types 
        WHERE source_type IS NOT NULL AND source_type != ''
        """
        substances_df = pd.read_sql_query(query_substances, conn)
        
        # Создаем словарь: вещество -> список его типов
        substance_types_dict = {}
        for _, row in substances_df.iterrows():
            if row['substance'] not in substance_types_dict:
                substance_types_dict[row['substance']] = []
            substance_types_dict[row['substance']].append(row['source_type'])
        
        # Основной запрос данных
        query = """
        SELECT 
            ae.section,
            ae.code,
            ae.substance,
            ae.value,
            ae.oktmo_code,
            ae.year,
            ic.indicator,
            lc.region,
            lc.municipal_district,
            lc.municipal_formation
        FROM air_emissions ae
        LEFT JOIN indicator_codes ic ON ae.code = ic.code
        LEFT JOIN location_codes lc ON ae.oktmo_code = lc.oktmo_code
        WHERE ae.value > 0  -- Только положительные значения
        """
        
        df = pd.read_sql_query(query, conn)
        
        # Преобразуем типы данных
        if 'year' in df.columns:
            df['year'] = pd.to_numeric(df['year'], errors='coerce')
        if 'value' in df.columns:
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # Добавляем информацию о типах веществ
        def get_substance_info(substance):
            if substance in substance_types_dict:
                types = substance_types_dict[substance]
                if len(types) == 1:
                    return types[0], types[0]  # название и отображение
                else:
                    # Если несколько типов, объединяем их
                    display_name = f"{substance} ({', '.join(types)})"
                    return types[0], display_name
            else:
                return substance, substance
        
        # Применяем функцию к каждому веществу
        substance_info = df['substance'].apply(get_substance_info)
        df['substance_name'] = substance_info.apply(lambda x: x[0])
        df['substance_display'] = substance_info.apply(lambda x: x[1])
        
        return df
        
    finally:
        conn.close()

# Функция для получения всех веществ с типами
@st.cache_data(ttl=3600)
def get_all_substances_with_types():
    """Загружает все вещества с их типами из БД"""
    conn = get_connection()
    try:
        query = """
        SELECT DISTINCT 
            st.substance,
            st.source_type as substance_type,
            COUNT(*) as count
        FROM substance_types st
        WHERE st.source_type IS NOT NULL AND st.source_type != ''
        GROUP BY st.substance, st.source_type
        ORDER BY st.substance, count DESC
        """
        df = pd.read_sql_query(query, conn)
        
        # Группируем по веществам
        substances_dict = {}
        for substance, group in df.groupby('substance'):
            types = group['substance_type'].tolist()
            if len(types) == 1:
                substances_dict[substance] = types[0]
            else:
                # Для веществ с несколькими типами создаем составное название
                substances_dict[substance] = f"{substance} ({', '.join(types[:2])}{'...' if len(types) > 2 else ''})"
        
        # Добавляем вещества из основной таблицы, которых нет в substance_types
        query_all_substances = "SELECT DISTINCT substance FROM air_emissions WHERE substance IS NOT NULL"
        all_substances_df = pd.read_sql_query(query_all_substances, conn)
        
        for substance in all_substances_df['substance']:
            if substance not in substances_dict:
                substances_dict[substance] = substance
        
        return substances_dict
    finally:
        conn.close()

# Функция для получения всех кодов с расшифровками
@st.cache_data(ttl=3600)
def get_all_codes_with_descriptions():
    """Загружает все коды с их расшифровками из БД"""
    conn = get_connection()
    try:
        query = """
        SELECT DISTINCT 
            code,
            indicator,
            COUNT(*) as count
        FROM indicator_codes 
        WHERE code IS NOT NULL AND code != ''
        GROUP BY code, indicator
        ORDER BY code
        """
        df = pd.read_sql_query(query, conn)
        
        # Создаем словарь: код -> список расшифровок (на случай дубликатов)
        codes_dict = {}
        for _, row in df.iterrows():
            code = row['code']
            indicator = row['indicator']
            
            if code not in codes_dict:
                codes_dict[code] = []
            codes_dict[code].append(indicator)
        
        # Для кодов с несколькими расшифровками объединяем их
        codes_display_dict = {}
        for code, indicators in codes_dict.items():
            if len(indicators) == 1:
                codes_display_dict[code] = f"{code} - {indicators[0]}"
            else:
                # Объединяем расшифровки через точку с запятой
                combined = f"{code} - {'; '.join(indicators[:2])}{'...' if len(indicators) > 2 else ''}"
                codes_display_dict[code] = combined
        
        return codes_dict, codes_display_dict
    finally:
        conn.close()

# Основной заголовок
st.title("Анализ выбросов загрязняющих веществ в атмосферный воздух")
st.markdown("---")

# Загрузка данных и словарей
try:
    with st.spinner("Загрузка данных из базы..."):
        df = load_data()
        all_substances = get_all_substances_with_types()
        all_codes, all_codes_display = get_all_codes_with_descriptions()
        
        # Отладочная информация
        with st.sidebar.expander("ℹ️ Отладка веществ и кодов"):
            st.write("**Вещества из БД:**")
            for substance, display_name in sorted(all_substances.items()):
                st.write(f"- {substance}: {display_name}")
            
            st.write("**Коды из БД:**")
            for code, display_name in sorted(all_codes_display.items()):
                st.write(f"- {display_name}")
            
except Exception as e:
    st.error(f"Ошибка загрузки данных: {e}")
    st.info("Проверьте, что файл базы данных 'air_emissions.db' существует в корне проекта.")
    st.stop()

# Боковая панель - фильтры
with st.sidebar:
    st.header("Фильтры данных")
    
    # 1. Фильтр по разделам
    st.subheader("Раздел данных")
    
    # Получаем уникальные разделы
    available_sections = sorted(df['section'].dropna().unique().astype(str))
    
    if len(available_sections) == 0:
        st.warning("Нет доступных разделов в данных")
        section = None
    else:
        # Показываем понятные названия
        section_options = []
        for sec in available_sections:
            section_options.append(f"{sec}")
        
        selected_section_display = st.selectbox(
            "Выберите раздел",
            options=section_options,
            index=0
        )
        
        # Извлекаем номер раздела
        section = selected_section_display.split(' - ')[0]
    
    # 2. Фильтр по годам
    st.subheader("Год")
    
    available_years = sorted(df['year'].dropna().unique().astype(int))
    
    if len(available_years) == 0:
        st.warning("Нет данных по годам")
        years = []
    else:
        years = st.multiselect(
            "Выберите год(ы)",
            options=available_years,
            default=[available_years[-1]] if available_years else []
        )
    
    # 3. Фильтр по уровню локации
    st.subheader("Уровень анализа")
    location_level = st.radio(
        "Группировать по:",
        options=['region', 'municipal_district'],
        format_func=lambda x: 'Региону' if x == 'region' else 'Муниципальному району',
        index=0
    )
    
    # 4. Фильтр по регионам
    st.subheader("Фильтр по регионам")
    
    available_regions = sorted(df['region'].dropna().unique())
    
    if len(available_regions) == 0:
        st.warning("Нет данных по регионам")
        selected_regions = []
    else:
        selected_regions = st.multiselect(
            "Выберите регионы",
            options=available_regions,
            default=available_regions[:3] if len(available_regions) >= 3 else available_regions
        )
    
    # 5. Фильтр по веществам
    st.subheader("Фильтр по веществам")
    
    # Получаем ВСЕ уникальные вещества из загруженных данных
    available_substances = sorted(df['substance'].dropna().unique())
    
    if len(available_substances) == 0:
        st.warning("Нет данных по веществам")
        selected_substances = []
    else:
        # Создаем список веществ с отображаемыми названиями
        substance_options = []
        for sub in available_substances:
            display_name = all_substances.get(sub, sub)
            substance_options.append((sub, display_name))
        
        # Сортируем по отображаемому названию
        substance_options.sort(key=lambda x: x[1])
        
        # Показываем в selectbox с отображаемыми названиями
        selected_display_names = st.multiselect(
            "Выберите вещества",
            options=[name for _, name in substance_options],
            default=[name for _, name in substance_options[:3]] if len(substance_options) >= 3 else [name for _, name in substance_options]
        )
        
        # Сопоставляем выбранные названия с кодами веществ
        display_to_code = {name: code for code, name in substance_options}
        selected_substances = [display_to_code[name] for name in selected_display_names if name in display_to_code]
    
    # 6. НОВЫЙ ФИЛЬТР: Фильтр по кодам (code)
    st.subheader("Фильтр по кодам показателей")
    
    if all_codes_display:
        # Получаем доступные коды из загруженных данных (с учетом фильтров выше)
        available_codes_in_data = sorted(df['code'].dropna().unique())
        
        if len(available_codes_in_data) == 0:
            st.warning("Нет доступных кодов в данных")
            selected_codes = []
        else:
            # Создаем список кодов с отображаемыми названиями
            code_options = []
            for code in available_codes_in_data:
                if code in all_codes_display:
                    display_name = all_codes_display[code]
                else:
                    display_name = f"{code} - (без расшифровки)"
                code_options.append((code, display_name))
            
            # Сортируем по отображаемому названию
            code_options.sort(key=lambda x: x[1])
            
            # Показываем в multiselect
            selected_code_displays = st.multiselect(
                "Выберите коды показателей",
                options=[name for _, name in code_options],
                default=None
            )
            
            # Сопоставляем выбранные названия с кодами
            display_to_code = {name: code for code, name in code_options}
            selected_codes = [display_to_code[name] for name in selected_code_displays if name in display_to_code]
            
    else:
        st.warning("Нет данных о кодах в базе")
        selected_codes = []
    
    # 7. Настройки отображения
    st.subheader("Настройки")
    chart_theme = st.selectbox(
        "Тема графиков",
        options=['plotly', 'plotly_white', 'plotly_dark', 'ggplot2', 'seaborn'],
        index=0
    )

# ФИЛЬТРАЦИЯ ДАННЫХ
df_filtered = df.copy()

# Применяем фильтры ТОЛЬКО если они выбраны
if section:
    df_filtered = df_filtered[df_filtered['section'] == section]

if years:
    df_filtered = df_filtered[df_filtered['year'].isin(years)]

if selected_regions:
    df_filtered = df_filtered[df_filtered['region'].isin(selected_regions)]

if selected_substances:
    df_filtered = df_filtered[df_filtered['substance'].isin(selected_substances)]

# Применяем фильтр по кодам
if selected_codes:
    df_filtered = df_filtered[df_filtered['code'].isin(selected_codes)]

# Основная панель
if df_filtered.empty:
    st.error("Нет данных для отображения с выбранными фильтрами!")
else:
    # ВЕРХНИЕ МЕТРИКИ
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_emissions = df_filtered['value'].sum() / 1000
        st.metric("Всего выбросов", f"{total_emissions:,.1f} тыс. тонн")
    
    with col2:
        avg_per_location = df_filtered.groupby(location_level)['value'].sum().mean() / 1000
        location_label = "региону" if location_level == 'region' else "району"
        st.metric(f"Средние по {location_label}", f"{avg_per_location:,.1f} тыс. тонн")
    
    with col3:
        locations_count = df_filtered[location_level].nunique()
        st.metric("Локаций", f"{locations_count:,}")
    
    with col4:
        years_count = df_filtered['year'].nunique()
        st.metric("Лет", f"{years_count}")
    
    
    st.markdown("---")
    
    # ВКЛАДКИ
    tab1, tab2, tab3, tab4 = st.tabs(["Основные диаграммы", "Динамика", "По регионам", "Детали"])
    
    with tab1:
        st.subheader("Столбчатые диаграммы выбросов")
        
        if len(years) == 1 or not years:
            # Группируем по локациям и веществам
            if location_level == 'region':
                # Используем substance_display для группировки
                df_grouped = df_filtered.groupby(['region', 'substance_display'])['value'].sum().reset_index()
                x_col = 'region'
                title = "Выбросы по регионам"
                x_label = 'Регион'
            else:
                df_grouped = df_filtered.groupby(['municipal_district', 'substance_display'])['value'].sum().reset_index()
                x_col = 'municipal_district'
                title = "Выбросы по муниципальным районам"
                x_label = 'Муниципальный район'
            
            # Берем топ-15 локаций для читаемости
            top_locations = df_grouped.groupby(x_col)['value'].sum().nlargest(15).index
            df_top = df_grouped[df_grouped[x_col].isin(top_locations)]
            
            # Проверяем на дублирование
            unique_substances = df_top['substance_display'].nunique()
            
            fig1 = px.bar(
                df_top,
                x=x_col,
                y='value',
                color='substance_display',
                barmode='group',
                title=title,
                labels={
                    'value': 'Выбросы (тонны)', 
                    x_col: x_label,
                    'substance_display': 'Вещество'
                },
                template=chart_theme
            )
            
        else:
            # Несколько лет - группируем по годам
            df_grouped = df_filtered.groupby(['year', 'substance_display'])['value'].sum().reset_index()
            
            # Проверяем на дублирование
            unique_substances = df_grouped['substance_display'].nunique()
            
            fig1 = px.bar(
                df_grouped,
                x='year',
                y='value',
                color='substance_display',
                barmode='group',
                title="Динамика выбросов по годам",
                labels={
                    'value': 'Выбросы (тонны)', 
                    'year': 'Год',
                    'substance_display': 'Вещество'
                },
                template=chart_theme
            )
        
        fig1.update_layout(
            height=500,
            xaxis_title=x_label if 'x_label' in locals() else 'Год',
            yaxis_title="Выбросы, тонны",
            hovermode='x unified',
            legend_title="Вещества"
        )
        st.plotly_chart(fig1, use_container_width=True)
        
        # Дополнительно: диаграмма по кодам
        if selected_codes:
            st.subheader("Распределение по выбранным кодам")
            
            code_grouped = df_filtered.groupby(['code', 'indicator'])['value'].sum().reset_index()
            code_grouped = code_grouped.sort_values('value', ascending=False)
            
            fig_codes = px.bar(
                code_grouped,
                x='code',
                y='value',
                title="Выбросы по кодам показателей",
                labels={'value': 'Выбросы (тонны)', 'code': 'Код показателя'},
                template=chart_theme,
                hover_data=['indicator']
            )
            fig_codes.update_layout(
                height=400,
                xaxis_title="Код показателя",
                yaxis_title="Выбросы, тонны"
            )
            st.plotly_chart(fig_codes, use_container_width=True)
    
    with tab2:
        st.subheader("Анализ динамики выбросов")
        
        if len(years) > 1:
            # Линейный график - используем substance_display для уникальности
            df_trend = df_filtered.groupby(['year', 'substance_display'])['value'].sum().reset_index()
            
            fig3 = px.line(
                df_trend,
                x='year',
                y='value',
                color='substance_display',
                markers=True,
                title="Динамика выбросов по годам",
                labels={
                    'value': 'Выбросы (тонны)', 
                    'year': 'Год',
                    'substance_display': 'Вещество'
                },
                template=chart_theme
            )
            fig3.update_layout(
                height=500,
                legend_title="Вещества"
            )
            st.plotly_chart(fig3, use_container_width=True)
            
            # Дополнительно: динамика по кодам
            if selected_codes:
                st.subheader("Динамика по выбранным кодам")
                
                code_trend = df_filtered.groupby(['year', 'code', 'indicator'])['value'].sum().reset_index()
                
                fig_codes_trend = px.line(
                    code_trend,
                    x='year',
                    y='value',
                    color='code',
                    markers=True,
                    title="Динамика выбросов по кодам",
                    labels={
                        'value': 'Выбросы (тонны)', 
                        'year': 'Год',
                        'code': 'Код показателя'
                    },
                    template=chart_theme,
                    hover_data=['indicator']
                )
                fig_codes_trend.update_layout(
                    height=400,
                    legend_title="Коды показателей"
                )
                st.plotly_chart(fig_codes_trend, use_container_width=True)
            
            # Анализ роста/спада
            st.subheader("Изменения по сравнению с предыдущим годом")
            
            changes_data = []
            # Используем оригинальные коды веществ для анализа
            unique_substances = df_filtered['substance'].unique()
            
            for substance in unique_substances:
                # Фильтруем по исходному коду вещества
                sub_df = df_filtered[df_filtered['substance'] == substance].groupby('year')['value'].sum().reset_index()
                sub_df = sub_df.sort_values('year')
                
                if len(sub_df) > 1:
                    for i in range(1, len(sub_df)):
                        prev = sub_df.iloc[i-1]['value']
                        curr_year = sub_df.iloc[i]['year']
                        curr_val = sub_df.iloc[i]['value']
                        change_pct = ((curr_val - prev) / prev * 100) if prev > 0 else 0
                        
                        display_name = all_substances.get(substance, substance)
                        
                        changes_data.append({
                            'Вещество': display_name,
                            'Год': curr_year,
                            'Изменение %': round(change_pct, 1),
                            'Выбросы, т': round(curr_val, 1),
                            'Тренд': 'Рост' if change_pct > 0 else 'Спад' if change_pct < 0 else 'Без изменений'
                        })
            
            if changes_data:
                changes_df = pd.DataFrame(changes_data)
                
                # Таблица изменений с цветовым кодированием
                st.dataframe(
                    changes_df,
                    column_config={
                        "Изменение %": st.column_config.ProgressColumn(
                            "Изменение %",
                            help="Изменение по сравнению с предыдущим годом",
                            format="%+.1f%%",
                            min_value=-100,
                            max_value=100,
                        ),
                        "Тренд": st.column_config.TextColumn(
                            "Тренд",
                            help="Направление изменения"
                        )
                    },
                    width='stretch',
                    height=300
                )
                
                # Сводная статистика по трендам
                growth_count = len([c for c in changes_data if c['Изменение %'] > 0])
                decline_count = len([c for c in changes_data if c['Изменение %'] < 0])
                stable_count = len([c for c in changes_data if c['Изменение %'] == 0])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Рост", f"{growth_count}")
                with col2:
                    st.metric("Спад", f"{decline_count}")
                with col3:
                    st.metric("Без изменений", f"{stable_count}")
        else:
            st.info("Выберите несколько лет для анализа динамики")
    
    with tab3:
        st.subheader("Географическое распределение выбросов")
        
        if location_level == 'region':
            # Общая сумма по регионам
            region_data = df_filtered.groupby('region')['value'].sum().reset_index()
            region_data = region_data.sort_values('value', ascending=True)
            
            fig5 = px.bar(
                region_data,
                x='value',
                y='region',
                orientation='h',
                title="Общие выбросы по регионам (сумма всех веществ)",
                labels={'value': 'Выбросы (тонны)', 'region': 'Регион'},
                template=chart_theme,
                color='value',
                color_continuous_scale='Viridis'
            )
            fig5.update_layout(
                height=600,
                coloraxis_colorbar_title="Выбросы, т"
            )
            st.plotly_chart(fig5, use_container_width=True)
            
        else:
            # Для муниципальных районов
            district_data = df_filtered.groupby(['region', 'municipal_district'])['value'].sum().reset_index()
            district_data = district_data.sort_values('value', ascending=False).head(20)
            
            fig6 = px.bar(
                district_data,
                x='value',
                y='municipal_district',
                color='region',
                orientation='h',
                title="Топ-20 муниципальных районов по выбросам",
                labels={'value': 'Выбросы (тонны)', 'municipal_district': 'Муниципальный район'},
                template=chart_theme
            )
            fig6.update_layout(height=600)
            st.plotly_chart(fig6, use_container_width=True)
    
    with tab4:
        st.subheader("Детальные данные")
        
        # Простая группировка для отображения с названиями веществ
        if location_level == 'region':
            detail_df = df_filtered.groupby(['region', 'code', 'indicator', 'substance', 'substance_display', 'year'])['value'].sum().reset_index()
        else:
            detail_df = df_filtered.groupby(['region', 'municipal_district', 'code', 'indicator', 'substance', 'substance_display', 'year'])['value'].sum().reset_index()
        
        # Форматирование
        detail_df['Выбросы (т)'] = detail_df['value'].round(2)
        detail_df['Выбросы (тыс. т)'] = (detail_df['value'] / 1000).round(3)
        
        # Убираем исходные колонки и переименовываем
        display_columns = ['region']
        if location_level == 'municipal_district':
            display_columns.append('municipal_district')
        display_columns.extend(['code', 'indicator', 'substance', 'substance_display', 'year', 'Выбросы (т)', 'Выбросы (тыс. т)'])
        
        display_df = detail_df[display_columns].rename(columns={
            'code': 'Код показателя',
            'indicator': 'Расшифровка кода',
            'substance': 'Код вещества',
            'substance_display': 'Вещество'
        })
        
        st.dataframe(
            display_df,
            width='stretch',
            height=400
        )
        
        # Статистика по данным
        st.subheader("Статистика по выбранным данным")
        
        # По веществам (используем отображаемые названия)
        if len(display_df) > 0:
            substance_stats = display_df.groupby('Вещество')['Выбросы (т)'].agg(['sum', 'mean', 'median', 'max']).round(1)
            substance_stats.columns = ['Сумма, т', 'Среднее, т', 'Медиана, т', 'Максимум, т']
            
            st.write("**Статистика по веществам:**")
            st.dataframe(substance_stats, width='stretch')
            
            # Статистика по кодам
            if selected_codes:
                code_stats = display_df.groupby('Код показателя')['Выбросы (т)'].agg(['sum', 'mean', 'median', 'max']).round(1)
                code_stats.columns = ['Сумма, т', 'Среднее, т', 'Медиана, т', 'Максимум, т']
                
                st.write("**Статистика по кодам показателей:**")
                st.dataframe(code_stats, width='stretch')

# Футер
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    <small>Система анализа выбросов | ETL: Dask + Prefect | Визуализация: Streamlit</small>
</div>
""", unsafe_allow_html=True)

# Кнопка обновления
with st.sidebar:
    st.markdown("---")
    if st.button("Обновить кэш данных"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()