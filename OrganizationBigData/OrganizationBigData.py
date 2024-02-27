import os
import requests
import tkinter as tk
from tkinter import ttk


class HDFSClient:
    def __init__(self, host, port, username):
        self.base_url = f"http://{host}:{port}/webhdfs/v1"
        self.username = username

    def mkdir(self, directory_name):
        """Создание каталога в HDFS."""
        url = f"{self.base_url}/{directory_name}?op=MKDIRS&user.name={self.username}"
        response = requests.put(url)
        return response.json()

    def put(self, local_file_name):
        """Загрузка файла в HDFS."""
        # Сначала получаем URL для загрузки
        temp_url = f"{self.base_url}/{local_file_name}?op=CREATE&overwrite=true&user.name={self.username}"
        init_response = requests.put(temp_url, allow_redirects=False)
        upload_url = init_response.headers['Location']  # Получаем URL для загрузки
        # Теперь загружаем файл
        with open(local_file_name, 'rb') as file_data:
            response = requests.put(upload_url, data=file_data)
        return response.status_code

    def get(self, hdfs_file_name):
        """Скачивание файла из HDFS."""
        temp_url = f"{self.base_url}/{hdfs_file_name}?op=OPEN&user.name={self.username}"
        init_response = requests.get(temp_url, allow_redirects=False)
        download_url = init_response.headers['Location']  # Получаем URL для загрузки
        response = requests.get(download_url)
        with open(hdfs_file_name, 'wb') as file:
            file.write(response.content)
        return "Downloaded"

    def append(self, local_file_name, hdfs_file_name):
        """Конкатенация файла в HDFS с локальным файлом."""
        temp_url = f"{self.base_url}/{hdfs_file_name}?op=APPEND&user.name={self.username}"
        init_response = requests.post(temp_url, allow_redirects=False)
        append_url = init_response.headers['Location']  # Получаем URL для добавления данных
        with open(local_file_name, 'rb') as file_data:
            response = requests.post(append_url, data=file_data)
        return response.status_code

    def delete(self, hdfs_file_name):
        """Удаление файла в HDFS."""
        url = f"{self.base_url}/{hdfs_file_name}?op=DELETE&user.name={self.username}"
        response = requests.delete(url)
        return response.json()

    def ls(self):
        """Отображение содержимого текущего каталога в HDFS."""
        url = f"{self.base_url}/?op=LISTSTATUS&user.name={self.username}"
        response = requests.get(url)
        return response.json()

    def cd(self, directory_name):
        """Переход в другой каталог в HDFS."""
        self.base_url = f"{self.base_url}/{directory_name}"
        # Это просто изменит базовый URL. В WebHDFS нет прямой команды для cd.

    def lls(self, local_directory="."):
        """Отображение содержимого текущего локального каталога."""
        files_and_dirs = os.listdir(local_directory)
        return files_and_dirs

    def lcd(self, directory_name):
        """Переход в другой локальный каталог."""
        os.chdir(directory_name)
        return os.getcwd()
    

class HDFSClientGUI:
    def __init__(self, master):
        self.master = master
        master.title("HDFS Client")

        self.client = HDFSClient('localhost', 50070, 'Taidanaishi')

        self.operation_var = tk.StringVar()
        self.operation_var.set("mkdir")

        self.operations = ["mkdir", "put", "get", "append", "delete", "ls", "cd", "lls", "lcd"]

        ttk.Label(master, text="Choose operation:").grid(column=0, row=0, sticky=tk.W)
        self.operation_menu = ttk.OptionMenu(master, self.operation_var, *self.operations)
        self.operation_menu.grid(column=1, row=0, sticky=tk.W)

        ttk.Label(master, text="Parameter 1:").grid(column=0, row=1, sticky=tk.W)
        self.param1_entry = ttk.Entry(master)
        self.param1_entry.grid(column=1, row=1, sticky=tk.EW)

        ttk.Label(master, text="Parameter 2:").grid(column=0, row=2, sticky=tk.W)
        self.param2_entry = ttk.Entry(master)
        self.param2_entry.grid(column=1, row=2, sticky=tk.EW)

        self.execute_button = ttk.Button(master, text="Execute", command=self.execute_operation)
        self.execute_button.grid(column=1, row=3, sticky=tk.EW)

        self.response_text = tk.Text(master, height=10, width=50)
        self.response_text.grid(column=0, row=4, columnspan=2, sticky=tk.EW)

    def execute_operation(self):
        operation = self.operation_var.get()
        param1 = self.param1_entry.get()
        param2 = self.param2_entry.get()

        try:
            if operation == "mkdir":
                response = self.client.mkdir(param1)
            elif operation == "put":
                response = self.client.put(param1)
            elif operation == "get":
                response = self.client.get(param1)
            elif operation == "append":
                response = self.client.append(param1, param2)
            elif operation == "delete":
                response = self.client.delete(param1)
            elif operation == "ls":
                response = self.client.ls()
            elif operation == "cd":
                response = self.client.cd(param1)
            elif operation == "lls":
                response = self.client.lls(param1)
            elif operation == "lcd":
                response = self.client.lcd(param1)
            else:
                response = "Unknown operation"

            self.response_text.delete(1.0, tk.END)
            self.response_text.insert(tk.END, str(response))
        except Exception as e:
            self.response_text.delete(1.0, tk.END)
            self.response_text.insert(tk.END, f"Error: {str(e)}")






# Пример использования
if __name__ == "__main__":
    root = tk.Tk()
    gui = HDFSClientGUI(root)
    root.mainloop()
