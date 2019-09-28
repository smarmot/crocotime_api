# подключение к Crocotime
CT_PARAMS = {
    'url': 'https://host.croco:8085/',
    'headers': {'Content-type': 'application/json; charset=utf-8'},
    'token': 'token',
    'app_version': '5.8.1'
}

# init
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATETIME_BEGIN = '2019-03-05 00:00:00'
DATETIME_END = '2019-03-05 23:59:59'

# количество процессов для multiprocessing.Pool
# настроить в зависимости от среды выполнения
PROCESSES = 10

days = 1
hours = 12  # для большой БД (более 300 сотрудников) установить 12 ч, а так можно и по 24 часа выгружать
INTERVAL = days * hours * 60 * 60

# если необходимо разбить выгрузу на секундные интервалы
SPLIT_PER_SECONDS = False

# Указать абсолютный путь к папке, в которую будут сохранены файды
PATH = r''
