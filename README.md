.. -*- mode: rst -*-

|PythonVersion|_ 

.. |PythonVersion| image:: https://img.shields.io/badge/python-3.8%20%7C%203.8%20%7C%203.9-blue
.. _PythonVersion: https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9-blue

Проект по банкам в рамках курса "Управление рисками в финансовых учреждениях"
-----------------------------------------------------------------------------------

Данный проект призван решить следующие задачи:

1. Автоматическое получение отчетности всех российских коммерческих банков с `соответствующей страницы <https://cbr.ru/banking_sector/otchetnost-kreditnykh-organizaciy/>`_ для форм 101, 102 за все доступные годы.
2. Агрегация полученных файлов в удобный для работы файл (для начала хотя бы единый `.csv` файл).
3. Группировка исходных счетов в отчетности банков в более крупные счета, что позволит получить аналитический баланс и отчет о прибылях и убытках для каждого банка, который есть в базе данных.
4. Автоматическое получение данных об отзывах лицензий коммерческих банков. Ручное добавление к этому набору банков, дефолт которых не проявился в виде отзыва лицензии.
5. Расчет по отчетности банков метрик, которые позволили бы спрогнозировать вероятность дефолта российских банков.
6. Оценивание модеей вероятности дефолта по имеющимся метрикам, выбор лучшей модели, отбор регрессоров.

В папке ``code`` лежат:

- Файл ``preprocessing.py``, который содержит все функции, необходимые для парсинга и группировки данных банковских отчетностей.
- Ноутбук ``data_preparation.ipynb``, который содержит пример парсинга отчетностей банков с сайта ЦБ и агрегации этих отчетностей.
- Файл `parameters.py`, который предназначен для хранения группировочных словарей и содержит пример такого словаря.

В папке ``data`` лежат:

- Файл ``BankDefaults.xlsx`` с информацией об отзывах лицензий банков.
- Файл ``macro`` с табличкой макропоказателей (пока файл не заполнен до конца).
- Папка ``grouping files``, в которой хранятся таблички Марины Александровны для группировки форм 101 и 102 в зависимости от изменений плана счетов в разные годы.

Дополнительные библиотеки, которые нужны для работы с проектом и не поставляются с ``Python``/``Anaconda`` по умолчанию, вы можете поставить командой 
``pip install tqdm requests pyunpack dbfread beautifulsoup4``

