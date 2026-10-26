import asyncio
import json
from datetime import datetime
import aiohttp

CAR_BRANDS_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/GetAllMakes?format=json"
CAR_SPECS_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/GetVehicleTypesForMakeId/440?format=json"

semaphore = asyncio.Semaphore(3)

async def fetch_data(session: aiohttp.ClientSession, url: str) -> dict | None:
    async with semaphore:
        for attempt in range(1, 4):
            try:
                async def _request():
                    async with session.get(url) as response:
                        if response.status == 200:
                            return await response.json()
                        return None

                data = await asyncio.wait_for(_request(), timeout=5.0)
                if data is not None:
                    return data
            except asyncio.TimeoutError:
                pass
            except aiohttp.ClientError:
                pass
            except json.JSONDecodeError:
                break
            except Exception:
                pass
            await asyncio.sleep(2)
        return None

async def collect_car_data() -> dict:
    async with aiohttp.ClientSession() as session:
        task_brands = fetch_data(session, CAR_BRANDS_URL)
        task_specs = fetch_data(session, CAR_SPECS_URL)
        brands_data, specs_data = await asyncio.gather(task_brands, task_specs)
        return {
            "timestamp": datetime.now().isoformat(),
            "car_brands": brands_data,
            "car_specs": specs_data,
        }

def save_to_json(data: dict, filename: str = "car_data.json") -> None:
    try:
        with open(filename, "a", encoding="utf-8") as file:
            file.write(json.dumps(data, ensure_ascii=False) + "\n")
    except Exception:
        pass

async def main():
    while True:
        data = await collect_car_data()
        save_to_json(data)
        await asyncio.sleep(600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

