# import sqlite3 as sq
# from create_bot import bot
# import json


# async def db_start():
#     global db, cur

#     db = sq.connect(r'BaseData/Psychotype_Test.db')
#     cur = db.cursor()
#     if db:
#         print('Psychotype_Test connected OK!')
#     cur.execute(
#         'CREATE TABLE IF NOT EXISTS Psychotype_Test(id INTEGER, question TEXT, answer TEXT)')
#     empty_db = cur.execute("""select * from Psychotype_Test""").fetchall()
#     if not empty_db:
#         with open(r'Table/questionnaire_schema.json', 'r', encoding='utf-8') as f:
#             data = json.load(f)
#         for i in range(60):
#             cur.execute("INSERT INTO Psychotype_Test VALUES(?, ?, ?)",
#                         (i, data[i]['text'][0]['value'], data[i]['answers'][0]['text'][0]['value']))
#             cur.execute("INSERT INTO Psychotype_Test VALUES(?, ?, ?)",
#                         (i, data[i]['text'][0]['value'], data[i]['answers'][1]['text'][0]['value']))
#         db.commit()


# async def get_question(i):
#     quest = cur.execute(f"""select question from Psychotype_Test
#                     where id = {i}
#                     limit 1""").fetchone()
#     return quest[0]


# async def get_answer(i):
#     answ = []
#     for ret in cur.execute(f"""select answer from Psychotype_Test
#                     where id = {i}
#                     limit 2""").fetchall():
#         answ.append(ret)
#     return answ
import asyncpg
from create_bot import bot
import json


async def async_db_start():
    global db, pool

    pool = await asyncpg.create_pool(
        host="192.168.0.116",
        port=5432,
        user="psyc_login",
        password="210459",
        database="psyc_db",
    )

    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute(
                '''
                CREATE TABLE IF NOT EXISTS Psychotype_Test(
                    id INTEGER,
                    question TEXT,
                    answer TEXT
                )
                '''
            )

            empty_db = await connection.fetch('''SELECT * FROM Psychotype_Test''')

            if not empty_db:
                with open(r'Table/questionnaire_schema.json', 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for i in range(60):
                    await connection.execute(
                        '''
                        INSERT INTO Psychotype_Test (id, question, answer)
                        VALUES ($1, $2, $3)
                        ''',
                        i,
                        data[i]['text'][0]['value'],
                        data[i]['answers'][0]['text'][0]['value']
                    )
                    await connection.execute(
                        '''
                        INSERT INTO Psychotype_Test (id, question, answer)
                        VALUES ($1, $2, $3)
                        ''',
                        i,
                        data[i]['text'][0]['value'],
                        data[i]['answers'][1]['text'][0]['value']
                    )


# async def get_question(i):
#     async with pool.acquire() as connection:
#         async with connection.transaction():
#             quest = await connection.fetchval(
#                 '''
#                 SELECT question FROM Psychotype_Test
#                 WHERE id = $1
#                 LIMIT 1
#                 ''',
#                 i
#             )
#             return quest


# async def get_answer(i):
#     async with pool.acquire() as connection:
#         async with connection.transaction():
#             answ = await connection.fetch(
#                 '''
#                 SELECT answer FROM Psychotype_Test
#                 WHERE id = $1
#                 LIMIT 2
#                 ''',
#                 i
#             )
#             print('gen_answer ', answ)
#             return [ret['answer'] for ret in answ]

# механизм блокировок SELECT FOR UPDATE, чтобы гарантировать,
# что только один пользователь может обновлять данные
# в определенный момент времени.
async def get_question(i):
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute('''
                LOCK TABLE Psychotype_Test IN SHARE ROW EXCLUSIVE MODE;
            ''')

            quest = await connection.fetchval(
                '''
                SELECT question FROM Psychotype_Test
                WHERE id = $1
                LIMIT 1
                FOR UPDATE;
                ''',
                i
            )
            return quest


async def get_answer(i):
    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.execute('''
                LOCK TABLE Psychotype_Test IN SHARE ROW EXCLUSIVE MODE;
            ''')

            answ = await connection.fetch(
                '''
                SELECT answer FROM Psychotype_Test
                WHERE id = $1
                LIMIT 2
                FOR UPDATE;
                ''',
                i
            )
            print('gen_answer ', answ)
            return [ret['answer'] for ret in answ]


# async def get_answer(i):
#     answ = []
#     for ret in cur.execute(f"""select answer from Psychotype_Test
#                     where id = {i}
#                     limit 2""").fetchall():
#         answ.append(ret)
#     return answ
