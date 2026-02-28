# Fundament Sparka – Python

Projekt z materiałami i ćwiczeniami do kursu podstaw Apache Spark w Pythonie (PySpark).

## Wymagania

- Python 3.12+
- PySpark 3.5.0

## Instalacja

1. Sklonuj repozytorium (lub przejdź do katalogu projektu):

   ```bash
   cd fundament-sparka-python
   ```

2. Utwórz i aktywuj środowisko wirtualne:

   ```bash
   python3 -m venv venv
   source venv/bin/activate   # Linux/macOS
   # venv\Scripts\activate    # Windows
   ```

3. Zainstaluj zależności:

   ```bash
   pip install -r requirements.txt
   ```

## Uruchomienie

### Moduł 1 – Wprowadzenie (DataFrame, SparkSession)

```bash
python module1/people.py
python module1/exercise.py
```

### Moduł 2 – RDD i wczytywanie danych

```bash
python module2/basic_operation1.py
python module2/basic_operation2.py
python module2/loaddata1.py
python module2/loaddata2.py
python module2/exercise.py
```

### Moduł 3 – Operacje na DataFrame (joiny, unie, JSON, agregacje)

```bash
python module3/people.py
python module3/joining1.py
python module3/joining2.py
python module3/unions.py
python module3/json_example.py
python module3/other_oper_pizza.py
python module3/other_oper_netflix.py
python module3/exercise1.py
python module3/exercise2.py
python module3/exercise3.py
```

**Uwaga:** Niektóre skrypty korzystają z plików z katalogu `data/` (CSV). Upewnij się, że katalog `data/` z plikami źródłowymi jest dostępny.

## Zawartość

### Moduł 1

| Ścieżka               | Opis |
|------------------------|------|
| `module1/people.py`   | Wprowadzenie do SparkSession i DataFrame – tworzenie, wyświetlanie, select. |
| `module1/exercise.py` | Zadanie z modułu 1 – DataFrame z danymi osób (PESEL, wiek, płeć, wynagrodzenie). |

### Moduł 2

| Ścieżka                       | Opis |
|-------------------------------|------|
| `module2/basic_operation1.py` | RDD – parallelize, map, filter na liście imion. |
| `module2/basic_operation2.py` | RDD – sum, reduce na liczbach. |
| `module2/loaddata1.py`        | Wczytywanie CSV (pizza_data.csv), header, inferSchema. |
| `module2/loaddata2.py`        | Filtrowanie DataFrame (pizze zawierające „Cheese”). |
| `module2/exercise.py`         | Zadanie z modułu 2 – RDD z danymi osób, sumy wieku (M/F). |

### Moduł 3

| Ścieżka                      | Opis |
|------------------------------|------|
| `module3/people.py`          | DataFrame – withColumn, concat_ws, pełna nazwa. |
| `module3/joining1.py`        | Joiny (left, inner) na DataFrame osób i zawodów. |
| `module3/joining2.py`        | Join z warunkiem (age >= ageLimit), agregacje (count, collect_list). |
| `module3/unions.py`          | union i unionByName na danych o pizzach. |
| `module3/json_example.py`    | Parsowanie JSON (from_json, StructType). |
| `module3/other_oper_pizza.py`| regexp_replace, groupBy, avg – ceny pizz wg firm. |
| `module3/other_oper_netflix.py` | na.fill, split, explode – dane Netflix (listed_in). |
| `module3/exercise1.py`       | Ćwiczenie – length, concat, join z limitem wieku. |
| `module3/exercise2.py`       | Ćwiczenie – Netflix (type, listed_in, regexp_extract). |
| `module3/exercise3.py`       | Ćwiczenie – pizze Medium, avg/min/max ceny. |

### Dane (`data/`)

Pliki CSV używane w przykładach: `pizza_data.csv`, `pizza_data_half.csv`, `pizza_data_half2.csv`, `netflix_titles.csv` oraz inne w katalogu `data/`.

## Licencja

Materiały do użytku w ramach kursu.
