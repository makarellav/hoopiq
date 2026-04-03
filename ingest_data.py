from datetime import date, timedelta, datetime
from urllib.parse import urlencode
import asyncio
import aiohttp
import asyncpg
from dotenv import load_dotenv
import os

load_dotenv()

headers = {
    "Ocp-Apim-Subscription-Key": os.getenv("SUBSCRIPTION_KEY"),
}

start_date = date(2024, 10, 22)
end_date = date(2025, 6, 22)


async def fetch_games_data(
    sess: aiohttp.ClientSession, sem: asyncio.Semaphore, url: str
):
    games_data = []

    async with sem:
        async with sess.get(url, headers=headers) as response:
            if response.status != 200:
                print(f"Failed to fetch {url}: Status {response.status}")

                return []

            data = await response.json()

            modules = data["modules"]

            # no games that day
            if not modules:
                return []

            cards = modules[0]["cards"]

            for card in cards:
                card_data = card["cardData"]

                season_type = card_data["seasonType"]

                if season_type not in ["Regular Season", "Playoffs", "PlayIn"]:
                    continue

                away_team_data = card_data["awayTeam"]
                home_tead_data = card_data["homeTeam"]

                game_data = {
                    "id": card_data["gameId"],
                    "game_start": datetime.fromisoformat(
                        card_data["actualStartTimeUTC"]
                    ),
                    "game_end": datetime.fromisoformat(card_data["actualEndTimeUTC"]),
                    "away_team_name": away_team_data["teamName"],
                    "away_team_score": away_team_data["score"],
                    "home_team_name": home_tead_data["teamName"],
                    "home_team_score": home_tead_data["score"],
                }

                games_data.append(game_data)

    return games_data


def build_urls(start_date, end_date):
    base_url = "https://core-api.nba.com/cp/api/v1.9/feeds/gamecardfeed"
    params = {"platform": "web"}

    urls = []
    current_date = start_date

    while current_date <= end_date:
        formatted_date = current_date.strftime("%m/%d/%Y")
        params["gamedate"] = formatted_date

        query_string = urlencode(params)

        urls.append(f"{base_url}?{query_string}")

        current_date += timedelta(days=1)

    return urls


async def get_data():
    urls = build_urls(start_date, end_date)

    sem = asyncio.Semaphore(5)

    async with aiohttp.ClientSession() as sess:
        tasks = [fetch_games_data(sess, sem, url) for url in urls]

        raw_data = await asyncio.gather(*tasks)

        data = [game for day_games in raw_data for game in day_games]

        return data


async def ingest_data():
    conn = await asyncpg.connect(
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
    )

    await conn.execute("DROP TABLE IF EXISTS games")

    await conn.execute("""CREATE TABLE games(
            id TEXT PRIMARY KEY,
            game_start TIMESTAMPTZ,
            game_end TIMESTAMPTZ,
            away_team_name TEXT,
            away_team_score SMALLINT,
            home_team_name TEXT,
            home_team_score SMALLINT
        )
    """)

    db_columns = [
        "id",
        "game_start",
        "game_end",
        "away_team_name",
        "away_team_score",
        "home_team_name",
        "home_team_score",
    ]

    data = await get_data()

    records = [tuple(game.get(col) for col in db_columns) for game in data]

    await conn.copy_records_to_table("games", records=records, columns=db_columns)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(ingest_data())
