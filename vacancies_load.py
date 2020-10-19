from time import time
import asyncio
import aiohttp
import json
vacancies_ids = []
vacancies = []

def save_ids(data):
    vacancies.append(data)

async def fetch_content(url, session):
    async with session.get(url, allow_redirects=True) as response:
        data = await response.json()
        save_ids(data)


async def main():
    tasks = []
    async with aiohttp.ClientSession() as session:
        for i in range(3000):
            url = 'https://api.hh.ru/vacancies/{}'.format(vacancies_ids[i])
            task = asyncio.create_task(fetch_content(url, session))
            tasks.append(task)

        await asyncio.gather(*tasks)

if __name__ == '__main__':
    with open('vacancies_ids.json', 'r') as file:
        vacancies_ids = json.load(file)
    t0 = time()
    asyncio.run(main())
    print(time() - t0)
    with open('vacancies.json', 'w', encoding='utf-8') as file:
        json.dump(vacancies, file, ensure_ascii=False)
