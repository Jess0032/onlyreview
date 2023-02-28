import logging
import os
import zipfile

import aiofiles
import aiohttp

logging.basicConfig(level=logging.INFO)


async def get_last_update(url, session):
    try:

        async with session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Request failed with status code: {resp.status}")

                return (await resp.text()).strip()

    except Exception as e:
        logging.exception(e)


async def download_files(url, zipfile, params=None, headers=None):
    try:

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Request failed with status code: {resp.status}")

                expected_size = int(resp.headers.get("Content-Length"), 0)
                async with aiofiles.open(zipfile, 'wb') as fd:
                    async for chunk in resp.content.iter_chunked(10 * (1024 ** 2)):
                        await fd.write(chunk)

    except Exception as e:
        logging.exception(e)

    finally:
        if expected_size == os.path.getsize("geoip.zip"):
            logging.info("Downloaded file size matches expected size. Download complete.")
            return
        logging.error("Downloaded file size does not match expected size, retrying...")
        return await download_files(url, params, headers)


def unzip_files(file_path, output_dir):
    with zipfile.ZipFile(file_path, mode="r") as archive:
        for file in archive.namelist():
            archive.extract(file, output_dir)
