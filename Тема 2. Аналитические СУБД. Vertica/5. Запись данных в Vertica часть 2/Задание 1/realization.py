import csv
from pathlib import Path

vertica_user = 'vt2511219940ea'

conn_info = {'host': 'vertica.data-engineer.education-services.ru', 
             'port': '5433',
             'user': 'vt2511219940ea',       
             'password': 'e796d5f0a2524f138a68e5efd8839362',
             'database': 'dwh',
             # Вначале он нам понадобится, а дальше — решите позже сами
            'autocommit': True
}

dataset = 'test_dataset.csv'
N = 10000 # на этот раз можете поставить даже 10 млн

with open(dataset, 'w') as csvfile:
    fwriter = csv.writer(csvfile, delimiter='|')
    for i in range(N):
        fwriter.writerow([i, 'asds'])

# эта команда напечатает абсолютный путь к файлу, скопируйте его
print(Path(dataset).resolve())

# а это пара первых строк для визуализации результата:
with open(dataset, 'r') as csvfile:
    for i in range(5):
        print(csvfile.readline(), end='')