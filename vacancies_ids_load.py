from time import time
import asyncio
import aiohttp
import json
vacancies_ids = []

def save_ids(data):
    print(data)
    for i in data['items']:
        vacancies_ids.append(i['id'])

async def fetch_content(url, session):
    async with session.get(url, allow_redirects=True) as response:
        data = await response.json()
        save_ids(data)


async def main():
    tasks = []
    jobs = ['python', 'java', 'c++']
    pages = 50
    async with aiohttp.ClientSession() as session:
        for job in jobs:
            for i in range(pages):
                url = 'https://api.hh.ru/vacancies?text={}&area=1&page={}'.format(job, i)
                task = asyncio.create_task(fetch_content(url, session))
                tasks.append(task)

        await asyncio.gather(*tasks)

if __name__ == '__main__':
    t0 = time()
    asyncio.run(main())
    print(time() - t0)
    with open('vacancies_ids_test.json', 'w') as file:
        json.dump(vacancies_ids, file)
