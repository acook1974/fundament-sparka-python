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

- **Podstawowy przykład (DataFrame):**
  ```bash
  python people.py
  ```

- **Ćwiczenie – Moduł 1:**
  ```bash
  python exercise.py
  ```

## Zawartość

| Plik          | Opis |
|---------------|------|
| `people.py`   | Wprowadzenie do SparkSession i DataFrame – tworzenie, wyświetlanie, select. |
| `exercise.py` | Zadanie z modułu 1 – DataFrame z danymi osób (PESEL, wiek, płeć, wynagrodzenie). |

## Licencja

Materiały do użytku w ramach kursu.
