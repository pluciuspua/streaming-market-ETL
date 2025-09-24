import asyncio
from publishers.publisher import PubSubPublisher
from publishers.publisher_crypto import stream_binance
from publishers.publisher_news import stream_news_data

async def main():
    news_publisher = PubSubPublisher(config_path="config.yaml", topic_id="topic_news")
    crypto_publisher = PubSubPublisher(config_path="config.yaml", topic_id="topic_crypto")
    loop = asyncio.get_running_loop()

    news_future = loop.run_in_executor(None, stream_news_data, news_publisher)
    crypto_task = asyncio.create_task(stream_binance(crypto_publisher))

    await asyncio.gather(news_future, crypto_task)

if __name__ == "__main__":
    asyncio.run(main())
