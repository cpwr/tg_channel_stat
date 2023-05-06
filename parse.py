import asyncio
import re

import httpx
import uvloop

from bs4 import BeautifulSoup

CHANNELS = [

]


class AsyncLeakyBucket:

    def __init__(self, max_tasks: int, time_period: float = 60):
        self._delay_time = time_period / max_tasks
        self._sem = asyncio.BoundedSemaphore(max_tasks)
        asyncio.create_task(self._leak_sem())

    async def _leak_sem(self):
        """
        Background task that leaks semaphore releases based on the desired rate of tasks per time_period
        """
        while True:
            await asyncio.sleep(self._delay_time)
            try:
                self._sem.release()
            except ValueError:
                pass

    async def __aenter__(self) -> None:
        await self._sem.acquire()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass


def compile_filename(page_url):
    suffix = ""
    if ".ru" in page_url:
        suffix = "_ru"

    return f"data/{page_url.split('/')[-1]}{suffix}.txt"


async def extract_tg_link_from_page(bucket: AsyncLeakyBucket, page_link: str) -> str | None:
    try:
        async with httpx.AsyncClient() as client:
            async with bucket:
                response = await client.get(page_link)
                response.raise_for_status()
    except Exception as e:
        print(e)
        print(f"Stats collection from tgstat skipped for {page_link}")
        return

    page_html = response.read()
    soup = BeautifulSoup(page_html, 'html.parser')
    tg_btn = soup.find("a", attrs={"href": re.compile('https://'), "role": "button", "rel": "nofollow"})
    print(tg_btn["href"])
    return tg_btn["href"]


async def parse_stat_page_urls(bucket: AsyncLeakyBucket, page_url: str):
    async with httpx.AsyncClient() as client:
        try:
            async with bucket:
                response = await client.get(page_url)
                response.raise_for_status()
        except Exception as e:
            print(e)
            print(f"FAILED: {page_url}")
            return

    page_html = response.read()
    soup = BeautifulSoup(page_html, 'html.parser')
    dropdown = soup.find("button", attrs={"class": "btn btn-light border dropdown-toggle text-truncate btn-sm"})
    if dropdown:
        el_cls = 'div[class="p-2"]'
    else:
        el_cls = 'div[class*="card card-body peer-item-box"]'

    tasks = []
    cells = soup.select(el_cls)
    for cell in cells:
        if el := cell.find("a", attrs={"href": re.compile('https://')}):
            task = asyncio.ensure_future(extract_tg_link_from_page(bucket, el["href"]))
            tasks.append(task)

    with open(compile_filename(page_url), 'w') as file:
        for task in asyncio.as_completed(tasks):
            result = await task
            if result:
                file.write(result + "\n")


async def main():
    tasks = []
    bucket = AsyncLeakyBucket(3, 15)
    for url in CHANNELS:
        task = asyncio.ensure_future(parse_stat_page_urls(bucket, url))
        tasks.append(task)

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
