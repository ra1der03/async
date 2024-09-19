import asyncio
import datetime

import aiohttp
from more_itertools import chunked
from models import init_orm, Session, SwapiPeople
max_requests = 3


async def get_people(http_session, person_id):
    response = await http_session.get(f"http://swapi.py4e.com/api/people/{person_id}/")
    data = await response.json()
    if data == {'detail': 'Not found'}:
        return {'name': 'None', 'height': 'None', 'mass': 'None', 'hair_color': 'None', 'skin_color': 'None',
                'eye_color': 'None', 'birth_year': 'None', 'gender': 'None', 'homeworld': 'None',
                'films': 'None', 'species': 'None', 'vehicles': 'None', 'starships': 'None'}
    [data.popitem() for j in range(3)]
    for i, n in data.items():
        list_ = []
        if 'http' in n:
            response = await http_session.get(n)
            data.update({i: (await response.json())['name']})
            continue
        if isinstance(n, list):
            for m in n:
                if i == 'films':
                    response = await http_session.get(m)
                    list_.append((await response.json())["title"])
                else:
                    response = await http_session.get(m)
                    list_.append((await response.json())["name"])
            data.update({i: ",".join(list_)})
    return data


async def insert_to_database(json_data):
    async with Session() as session:
        if not json_data:
            orm_objects = SwapiPeople(name=None, height=None, mass=None, hair_color=None,
                skin_color=None, eye_color=None, birth_year=None,
                gender=None, homeworld=None, films=None, species=None,
                vehicles=None, starships=None)
        else:
            orm_objects = [SwapiPeople(**json) for json in json_data]
        session.add_all(orm_objects)
        await session.commit()


async def main():
    await init_orm()
    http_session = aiohttp.ClientSession()
    for chunk_i in chunked(range(1, 100), max_requests):
        coros = [get_people(http_session, i) for i in chunk_i]
        result = await asyncio.gather(*coros)
        asyncio.create_task(insert_to_database(result))

    await http_session.close()
    tasks = asyncio.all_tasks()
    current_task = asyncio.current_task()
    tasks.remove(current_task)
    await asyncio.gather(*tasks, return_exceptions=True)


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)