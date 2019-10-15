# Auto Tests for Analytics

### Для работы над проектом:

1. Склонируйте проект в нужную папку: 

`git clone https://github.com/alextimakov/auto_tests.git`

2. Распакуйте проект и создайте виртуальное окружение в нём:

`cd ./auto_tests`

`python3 -m venv ./`

3. Зайдите в созданное окружение и установите pip-tools:

`source .\venv\Scripts\Activate`

`pip install pip-tools`

4. Установите в окружение необходимые зависимости:

`pip-sync`


### Возникающие ошибки

1. Ошибка pip
- При установке пакетов через pip возникает ошибка типа 
`pip install fails with “connection error: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:598)”`. 

- Решаем с помощью установки в обход сертификатов:
`pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org <package_name>`

- Работает и в случае с прямой установкой через pip, так и через pip-sync
