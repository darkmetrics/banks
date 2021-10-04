.. -*- mode: rst -*-

|PythonVersion|_ 

.. |PythonVersion| image:: https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9-blue
.. _PythonVersion: https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9-blue

Проект по банкам в рамках курса "Управление рисками в финансовых учреждениях"
-----------------------------------------------------------------------------------

Данный проект призван решить следующие задачи:
1. Автоматическое получение отчетности всех российских коммерческих банков с [соответствующей страницы](https://cbr.ru/banking_sector/otchetnost-kreditnykh-organizaciy/) для форм 101, 102 за все доступные годы.
2. Агрегация полученных файлов в удобный для работы файл (для начала хотя бы единый `.csv` файл).
3. Группировка исходных счетов в отчетности банков в более крупные счета, что позволит получить аналитический баланс и отчет о прибылях и убытках для каждого банка, который есть в базе данных.
4. Автоматическое получение данных об отзывах лицензий коммерческих банков. Ручное добавление к этому набору банков, дефолт которых не проявился в виде отзыва лицензии.
5. Расчет по отчетности банков метрик, которые позволили бы спрогнозировать вероятность дефолта российских банков.
6. Оценивание модеей вероятности дефолта по имеющимся метрикам, выбор лучшей модели, отбор регрессоров.

