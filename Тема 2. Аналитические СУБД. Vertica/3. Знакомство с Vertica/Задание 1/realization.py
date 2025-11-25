import vertica_python

conn_info = {'host': 'vertica.data-engineer.education-services.ru', 
             'port': '5433',
             'user': 'vt2511219940ea',       
             'password': 'e796d5f0a2524f138a68e5efd8839362',
             'database': 'dwh',
             # Вначале он нам понадобится, а дальше — решите позже сами
            'autocommit': True
}

N = 10000
batch = 5

with vertica_python.connect(**conn_info) as conn:
    curs = conn.cursor()
    insert_stmt = 'INSERT INTO BAD_IDEA VALUES ({},\'a\');'
    
    for i in range(0, N, batch):
        # будем отправлять сразу по несколько команд
        curs.execute(
            '\n'.join(
                [insert_stmt.format(i + j) for j in range(batch)])
        )
        
    curs.commit()

def try_select(conn_info=conn_info):
    # И рекомендуем использовать соединение вот так
    with vertica_python.connect(**conn_info) as conn:
        # Select 1 — ваш код здесь; 
        cur = conn.cursor()
        cur.execute('SELECT 1 as a1')
        res = cur.fetchall()
        
        return res