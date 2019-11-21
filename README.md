# Auto Tests for Analytics

### Для работы над проектом:

1. Склонируйте проект в нужную папку: 

```bash
git clone https://github.com/alextimakov/auto_tests.git
```


2. Создайте в папке виртуальное окружение:

```bash
cd ./auto_tests
python -m venv venv -- для windows
python3 -m venv venv -- для ubuntu
```

3. Активируйте окружение и установите в нём pip-tools:

```bash
.\venv\Scripts\activate -- для windows
source venv/bin/activate -- для ubuntu
pip install pip-tools
```

4. Установите в окружение необходимые зависимости:

```bash
pip-sync
```

5. Для запуска зайдите в родительскую папку и запустите файл `main.py` с нужными флагами:

```bash
-l login -- введите логин, если он отличается от системного
-p password -- введите пароль, сделано для удобства запуска из shell
-t test_mode -- если True, то ограничивает кол-во запускаемых метрик до 3-х

python3 auto_tests/main.py -- для ubuntu
python .\auto_tests\main.py -- для windows

python .\auto_tests\main.py -l your_login -t True -- запуск с указанным логином в тестовом режиме
```


### Возникающие ошибки

#### Ошибка pip
- При установке пакетов через pip возникает ошибка типа: 
`pip install fails with “connection error: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:598)”`. 

- Решаем с помощью установки в обход сертификатов:
`pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org <package_name>`

- Работает и в случае с прямой установкой через pip, так и через `pip-sync`


### Правила работы над проектом
1. Все адреса, логины и пароли тянутся из `config.py`. 
Он включен в `.gitconfig` не просто так, просьба НЕ ПУШИТЬ его в репозиторий.
Темплейт конфигурации - в `config_examply.py`.

2. Версионирование проекта - по [semver](https://semver.org/).
Позднее настроим автоматическую накатку версий. 

3. Не забываем о правилах работы с удалённым репозиторием:
    - Обязательно - делать `git pull` перед началом работ и мержить конфликты перед каждым пушем
    - Если возникает мёрж конфликт, то не забываем пообщаться с автором конфликтующей версии и только после этого убивать его кусок кода
    
4. Просьба не запускать скрипт в пиковые часы и на всех метриках

5. При запуске (особенно при тестировании) не забываем добавлять соответствующие флаги

### Механика работы
1. Переменные в каждый из URL'ов (prod \ qa \ etc) подтягиваются из их же БД

2. Задан принудительный лимит на доступное время поллинга метрики - 120 секунд

3. Аутентификация осуществляется при помощи [python-keycloak](https://github.com/marcospereirampj/python-keycloak)
 