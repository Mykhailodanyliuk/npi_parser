import asyncio
import datetime
import time

import aiohttp
import jellyfish
import pymongo

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
}


def get_collection_from_db(data_base, collection, client):
    db = client[data_base]
    return db[collection]


async def get_page(session, url):
    while True:
        try:
            async with session.get(url, headers=headers) as r:
                # print(r.status)
                if r.status == 200:
                    return await r.json()
        except asyncio.exceptions.TimeoutError:
            print('asyncio.exceptions.TimeoutError')


async def get_all(session, urls):
    tasks = []
    for url in urls:
        task = asyncio.create_task(get_page(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results


async def get_all_data_urls(urls, limit=5000):
    timeout = aiohttp.ClientTimeout(total=600)
    connector = aiohttp.TCPConnector(limit=limit)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        data = await get_all(session, urls)
        return data


def write_all_data_parser3():
    update_collection = get_collection_from_db('db', 'update_collection', client)
    npi_data = get_collection_from_db('db', 'npi_data', client)
    sec_tickers_collection = get_collection_from_db('db', 'sec_data_tickers', client)
    last_len_records = npi_data.estimated_document_count()

    ciks = [c_json.get('cik_str').lstrip('0') for c_json in sec_tickers_collection.find({})]
    ciks = list(set(ciks))
    links1 = [
        f'https://api.orb-intelligence.com/3/search/?api_key=c66c5dad-395c-4ec6-afdf-7b78eb94166a&limit=10&cik={cik}'
        for cik in ciks]
    fisrt_data1 = asyncio.run(get_all_data_urls(links1, 2))
    second_links = []
    unsearched_ciks = []
    for data1 in fisrt_data1:
        if data1.get('results_count') == 1:
            second_links.append(data1.get('results')[0].get('fetch_url'))
        else:
            unsearched_ciks.append(data1.get('request_fields').get('cik'))
    link_name_list = []

    for unsearched_cik in unsearched_ciks:
        searched_company = sec_tickers_collection.find_one({'cik_str': unsearched_cik.zfill(10)})
        for ticker in searched_company.get("tickers"):
            link_name_list.append([
                f'https://api.orb-intelligence.com/3/search/?api_key=c66c5dad-395c-4ec6-afdf-7b78eb94166a&limit=10&ticker={ticker.replace("-", "")}',
                searched_company.get("title")])

    links2 = [block[0] for block in link_name_list]
    fisrt_data2 = asyncio.run(get_all_data_urls(links2, 2))
    for index, data in enumerate(fisrt_data2):
        for results_data in data.get('results'):
            if jellyfish.jaro_winkler_similarity(results_data.get('name').lower(),
                                                 link_name_list[index][1].lower()) > 0.85:
                second_links.append(results_data.get('fetch_url'))
                break
    second_data = asyncio.run(get_all_data_urls(second_links, 2))
    cik_npi_list = [[data.get('cik'), data.get('npis')] for data in second_data if data.get('npis') != []]
    for cik, npi in cik_npi_list:
        print(cik)
        npi_update_query = {'cik': cik, 'npi': npi, 'upload_at': datetime.datetime.now()}
        if npi_data.find_one({'cik': cik}):
            npi_data.update_one({'cik': cik}, {"$set": npi_update_query})
        else:
            npi_data.insert_one(npi_update_query)

    total_records = npi_data.estimated_document_count()
    update_query = {'name': 'npi_data', 'new_records': total_records - last_len_records,
                    'total_records': total_records,
                    'update_date': datetime.datetime.now()}
    if update_collection.find_one({'name': 'npi_data'}):
        update_collection.update_one({'name': 'npi_data'}, {"$set": update_query})
    else:
        update_collection.insert_one(update_query)


if __name__ == '__main__':
    client = pymongo.MongoClient('mongodb://localhost:27017')
    while True:
        start_time = time.time()
        write_all_data_parser3()
        work_time = int(time.time() - start_time)
        time.sleep(abs(work_time % 14400 - 14400))
