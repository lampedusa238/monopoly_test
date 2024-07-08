### 1. Инициализация нового Git-репозитория
```bash
git init
```

### 2. Cоздание ветки main

```bash
# Создание ветки main
git checkout -b main

# Добавляет содержимое рабочего каталога в индекс (staging area) для последующего коммита
git add .

# Создание первого коммита, для сохранения текущего состояние репозитория
git commit -m "first commit"

# Создание ветки develop
git branch develop
```

### 3. Создание ветки feature/add-dag от ветки develop
```bash
# Переход на ветку develop
git checkout develop

# Создание ветки feature/add-dag от ветки develop
git checkout -b feature/add-dag
```

### 4. Добавление DAG файла

```bash
# Добавление DAG-файла в индекс (staging area) для последующего коммита
git add weather_data_pipeline_dag.py 

# Создание коммита 
git commit -m "Add initial DAG for weather data pipeline"

```


### 5. Отправка веток в удаленный репозиторий
```bash
# Добавление удаленного репозитория с именем origin
git remote add origin https://github.com/lampedusa238/monopoly_test.git

# Отправка всех локальных веток в удаленный репозиторий
git push --all origin

```


Для создания pull request (PR) из ветки feature/add-dag в develop, объединения PR после проверки и одобрения использовался веб-интерфейс GitHub. 