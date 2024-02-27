import requests
import sys
import os

# 1. Функция для выполнения команды mkdir
def mkdir(directory):
    # Определение URL-адреса для создания директории в HDFS
    url = f"http://{server}:{port}/webhdfs/v1/{directory}?op=MKDIRS&user.name={username}"
    # Выполнение HTTP-запроса типа PUT для создания директории
    response = requests.put(url)
    # Проверка статусного кода ответа
    if response.status_code == 200:
        # Если статусный код 200, выводится сообщение об успешном создании директории
        print(f"Директория {directory} была успешно создана.")
    else:
        # В противном случае выводится сообщение о неудаче создания директории
        print("Не удалось создать директорию.")

# 2. Функция для выполнения команды put
def put(local_file, hdfs_file):
    # Определение URL-адреса для создания файла в HDFS
    url = f"http://{server}:{port}/webhdfs/v1/{hdfs_file}?op=CREATE&user.name={username}"
    # Выполнение HTTP-запроса типа PUT для создания файла
    response = requests.put(url, allow_redirects=False)
    # Проверка статусного кода ответа
    if response.status_code == 307:
        # # Если статусный код 307, получение URL для загрузки файла
        redirect_url = response.headers['Location']
        # Загрузка файла по полученному URL
        with open(local_file, 'rb') as file:
            response = requests.put(redirect_url, data=file)
        # Проверка статусного кода ответа после загрузки файла
        if response.status_code == 201:
            # Если статусный код 201, выводится сообщение об успешной загрузке файла
            print(f"Файл {local_file} был успешно загружен в {hdfs_file}.")
        else:
            # В противном случае выводится сообщение о неудаче загрузки файла
            print("Не удалось загрузить файл.")
    else:
        # Если статусный код не равен 307, выводится сообщение о неудаче создания файла в HDFS
        print("Не удалось создать файл в HDFS.")

# 3. Функция для выполнения команды get
def get(hdfs_file, local_file):
    # Определение URL-адреса для получения файла из HDFS
    url = f"http://{server}:{port}/webhdfs/v1/{hdfs_file}?op=OPEN&user.name={username}"
    # Выполнение HTTP-запроса типа GET для получения файла
    response = requests.get(url)
    # Проверка статусного кода ответа
    if response.status_code == 200:
        # Если статусный код 200, открываем локальный файл для записи в двоичном режиме
        with open(local_file, 'wb') as file:
            # Записываем содержимое ответа в локальный файл
            file.write(response.content)
        # Выводится сообщение об успешном скачивании и сохранении файла
        print(f"Файл {hdfs_file} был успешно скачан и сохранен как {local_file}.")
    else:
        # Если статусный код не равен 200, выводится сообщение о неудаче скачивания файла
        print("Не удалось скачать файл.")

# 4. Функция для выполнения команды append
def append(local_file, hdfs_file):
    # Определение URL-адреса для добавления данных в файл в HDFS
    url = f"http://{server}:{port}/webhdfs/v1/{hdfs_file}?op=APPEND&user.name={username}"
    # Выполнение HTTP-запроса типа POST для добавления данных в файл
    response = requests.post(url, allow_redirects=False)
    # Проверка статусного кода ответа
    if response.status_code == 307:
        # Если статусный код 307, получение URL для добавления данных в файл
        redirect_url = response.headers['Location']
        # Отправка данных из локального файла по полученному URL
        with open(local_file, 'rb') as file:
            response = requests.post(redirect_url, data=file)
        # Проверка статусного кода ответа после добавления данных в файл
        if response.status_code == 200:
            # Если статусный код 200, выводится сообщение об успешном добавлении данных в файл
            print(f"Данные из файла {local_file} были успешно добавлены в {hdfs_file}.")
        else:
            # В противном случае выводится сообщение о неудаче добавления данных в файл
            print("Не удалось добавить данные в файл.")
    else:
        # Если статусный код не равен 307, выводится сообщение о неудаче добавления данных в файл
        print("Не удалось добавить данные в файл.")

# 5. Функция для выполнения команды delete
def delete(hdfs_file):
    # Определение URL-адреса для удаления файла или директории в HDFS
    url = f"http://{server}:{port}/webhdfs/v1/{hdfs_file}?op=DELETE&user.name={username}"
    # Выполнение HTTP-запроса типа DELETE для удаления файла или директории
    response = requests.delete(url)
    # Проверка статусного кода ответа
    if response.status_code == 200:
        # Если статусный код 200, выводится сообщение об успешном удалении файла или директории
        print(f"Файл или директория {hdfs_file} были успешно удалены.")
    else:
        # Если статусный код не равен 200, выводится сообщение о неудаче удаления файла или директории
        print("Не удалось удалить файл или директорию.")

# 6. Функция для выполнения команды ls
def ls():
    # Определение URL-адреса для получения списка файлов и директорий в текущем каталоге HDFS
    url = f"http://{server}:{port}/webhdfs/v1/{current_directory}?op=LISTSTATUS&user.name={username}"
    # Выполнение HTTP-запроса типа GET для получения списка файлов и директорий
    response = requests.get(url)
    # Преобразование ответа в формат JSON
    data = response.json()
    # Проверка наличия ключа 'FileStatuses' в данных
    if 'FileStatuses' in data:
        # Если ключ 'FileStatuses' присутствует, получаем список статусов файлов
        file_statuses = data['FileStatuses']['FileStatus']
        # Проходим по каждому статусу файла в списке
        for file_status in file_statuses:
            # Извлекаем имя файла из статуса
            file_name = file_status['pathSuffix']
            # Извлекаем тип файла из статуса
            file_type = file_status['type']
            # Проверяем тип файла
            if file_type == 'DIRECTORY':
                # Если тип файла - директория, выводим соответствующее сообщение
                print("[Директория]", file_name)
            else:
                # В противном случае, если тип файла - файл, выводим соответствующее сообщение
                print("[Файл]", file_name)
    else:
        # Если ключ 'FileStatuses' отсутствует, выводим сообщение о том, что текущий каталог не существует или пуст
        print("Текущий каталог не существует или пуст.")

# 7.1. Глобальная переменная для текущего рабочего каталога в HDFS
current_directory = "/"

# 7. Функция для выполнения команды cd
def cd(directory):
    # Использование глобальной переменной current_directory
    global current_directory
    if directory == "..":
        # Если указано "..", выполняется переход на уровень выше
        url = f"http://{server}:{port}/webhdfs/v1/{current_directory}?op=GETFILESTATUS&user.name={username}"
        response = requests.get(url)
        data = response.json()
        if 'FileStatus' in data:
            # Если ключ 'FileStatus' присутствует в данных
            parent_dir = data['FileStatus']['pathSuffix']
            # Извлекается родительский каталог текущего каталога
            current_directory = os.path.dirname(current_directory.rstrip('/'))
            print(f"Переход на уровень выше. Текущий каталог: {current_directory}")
        else:
            # Если ключ 'FileStatus' отсутствует, выводится сообщение о невозможности перехода на уровень выше
            print("Невозможно перейти на уровень выше.")
    else:
        # Если указано другое имя каталога, выполняется переход в указанный каталог в HDFS
        url = f"http://{server}:{port}/webhdfs/v1/{current_directory}/{directory}?op=LISTSTATUS&user.name={username}"
        response = requests.get(url)
        data = response.json()
        if 'FileStatuses' in data:
            # Если ключ 'FileStatuses' присутствует в данных, выводится соответствующее сообщение
            current_directory = os.path.join(current_directory, directory)
            print(f"Переход в каталог {current_directory}")
        else:
            # Если ключ 'FileStatuses' отсутствует, выводится сообщение о том, что указанный каталог не существует
            print("Указанный каталог не существует.")

# 8. Функция для выполнения команды lls
def lls():
    # Отображение содержимого текущего локального каталога
    for entry in os.listdir():
        if os.path.isdir(entry):
            # Если entry является директорией, выводится соответствующее сообщение
            print(f"[Директория] {entry}")
        else:
            # Если entry является файлом, выводится соответствующее сообщение
            print(f"[Файл] {entry}")

# 9. Функция для выполнения команды lcd
def lcd(directory):
    if directory == "..":
        # Переход на уровень выше
        os.chdir("..")
        print(f"Переход на уровень выше. Текущий локальный каталог: {os.getcwd()}")
    else:
        # Переход в указанный локальный каталог
        if os.path.exists(directory) and os.path.isdir(directory):
            # Если указанный каталог существует и является директорией, выполняется переход
            os.chdir(directory)
            print(f"Переход в локальный каталог: {os.getcwd()}")
        else:
            # Если указанный каталог не существует или не является директорией, выводится сообщение
            print("Указанный каталог не существует.")

# 10. Получение параметров командной строки
server = sys.argv[1]    # localhost
port = sys.argv[2]      # 50070
username = sys.argv[3]  # bonbison

# 11. Список доступных команд
commands = {
    "mkdir": mkdir,
    "put": put,
    "get": get,
    "append": append,
    "delete": delete,
    "ls": ls,
    "cd": cd,
    "lls": lls,
    "lcd": lcd
}

# 12. Вывод списка доступных команд и синтаксиса их использования
print("Доступные команды:\n► Создание директории в HDFS: mkdir <directory>\n► Загрузка файла в HDFS: put <local_file> <hdfs_file>"
      "\n► Скачивание файла из HDFS: get <hdfs_file> <local_file>\n► Конкатенация файла в HDFS с локальным файлом: append <local_file> <hdfs_file>"
      "\n► Удаление файла из HDFS: delete <hdfs_file>\n► Отображение содержимого текущего каталога в HDFS с разделением файлов и каталогов: ls"
      "\n► Переход в другой каталог в HDFS (.. — на уровень выше): cd <directory>\n► Отображение содержимого текущего локального каталога "
      "с разделением файлов и каталогов: lls\n► Переход в другой локальный каталог (.. — на уровень выше): lcd <directory>\n")

# 13. Основной цикл программы
while True:
    # Получение команды от пользователя и разделение её на отдельные части
    command = input("Введите команду: ").split()
    # Если первая часть команды присутствует в словаре commands
    if command[0] in commands:
        # И если команда имеет дополнительные аргументы
        if len(command) > 1:
            # То происходит вызов функции из словаря commands с передачей дополнительных аргументов
            commands[command[0]](*command[1:])
        # Если же команда не имеет дополнительных аргументов
        else:
            # То происходит вызов функции из словаря commands без дополнительных аргументов
            commands[command[0]]()
    # Если первая часть команды отсутствует в словаре commands
    else:
        # Выводится сообщение о неверной команде
        print("Неверная команда. Попробуйте снова.")
