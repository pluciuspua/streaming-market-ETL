from publishers.publisher import PubSubPublisher
from publishers.publisher_crypto import stream_binance
from publishers.publisher_news import stream_news_data
from aiohttp import web
import asyncio
import yaml
import os

with open("config.yaml", 'r') as stream:
    cfg = yaml.safe_load(stream)
    if cfg:
        gcp_project = cfg["gcp_project_id"]
        gcp_region = cfg["gcp_region"]


async def _main():
    news_publisher = PubSubPublisher(config_path="config.yaml", topic_id="topic_news")
    crypto_publisher = PubSubPublisher(config_path="config.yaml", topic_id="topic_crypto")
    loop = asyncio.get_running_loop()

    news_future = loop.run_in_executor(None, stream_news_data, news_publisher)
    crypto_task = asyncio.create_task(stream_binance(crypto_publisher))

    # Start web server
    app = web.Application()
    app.add_routes([web.get("/", health)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, port=int(os.environ.get("PORT", 8080)))
    await site.start()

    async def handle_task(task, name):
        try:
            await task
        except Exception as e:
            print(f"{name} failed: {e}")

    try:
        await asyncio.gather(
            handle_task(news_future, "News publisher"),
            handle_task(crypto_task, "Crypto publisher"),
            return_exceptions=True
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("Shutting down publishers...")
# Health check endpoint
async def health(request):
    return web.Response(text="ok")

def start():
    app = web.Application()
    app.add_routes([web.get("/", health)])
    web.run_app(app, port=int(os.environ.get("PORT", 8080)))

if __name__ == "__main__":
    asyncio.run(_main())