import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from check_update import periodic_check, set_local_update_date
from src.GeoIP_Updater.configuration import minutes, base_dir

if __name__ == "__main__":
    scheduler = AsyncIOScheduler()
    set_local_update_date("last_update_date", "")
    scheduler.add_job(periodic_check, args=[base_dir], trigger='interval', minutes=minutes)
    scheduler.start()
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
