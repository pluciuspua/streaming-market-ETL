from publisher_crypto import stream_binance
from publisher_news import stream_news_data
import asyncio


async def _main():
    loop = asyncio.get_running_loop()
    # run the blocking news publisher in a thread/executor
    news_future = loop.run_in_executor(None, stream_news_data)
    # run the async crypto publisher as a task
    crypto_task = asyncio.create_task(stream_binance())

    try:
        await asyncio.gather(news_future, crypto_task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("Shutting down publishers...")


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        print("Stopped by user")
