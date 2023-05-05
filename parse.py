import asyncio
import json
import re

import httpx
import uvloop

from bs4 import BeautifulSoup, SoupStrainer

CHANNELS = []


async def parse_page(page_url: str):
    await asyncio.sleep(1)
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(page_url)
            response.raise_for_status()
        except Exception as e:
            print(e)
            print(f"FAILED: {page_url}")

    page_html = response.read()
    only_item_cells = SoupStrainer("div", attrs={"class": "card card-body peer-item-box py-2 border mb-2 mb-sm-3 border-info-hover position-relative "})

    soup = BeautifulSoup(page_html, 'html.parser', parse_only=only_item_cells)

    links = []
    for el in soup.find_all("a", attrs={"href": re.compile('https://')}):
        links.append(el["href"])

    write_to_file(file_name=f"data/{page_url.split('/')[-1]}.txt", data="\n".join(links))


def write_to_file(file_name: str, data: dict | list | str) -> None:
    with open(file_name, 'a') as file:
        match data:
            case dict() | list():
                file.write(json.dumps(data))
            case str():
                file.write(data)


async def main():
    tasks = []
    for url in CHANNELS:
        task = asyncio.ensure_future(parse_page(url))
        tasks.append(task)

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
