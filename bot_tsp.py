import asyncio
import logging

from aiogram import Bot, Dispatcher

from bot.main import preflight_driver_startup, router_main
from config import get_settings


async def main() -> None:
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.bot.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    bot = Bot(token=settings.bot.bot_token)
    dispatcher = Dispatcher()
    dispatcher.include_router(router_main)

    await preflight_driver_startup()
    await bot.delete_webhook(drop_pending_updates=True)
    await dispatcher.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
