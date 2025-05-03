import requests
import logging


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id

    def send_message(self, text: str):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        data = {"chat_id": self.chat_id, "text": text}
        try:
            requests.post(url, data=data)
        except Exception as e:
            logging.error(f"Telegram message error: {e}")

    def send_photo(self, image_path: str, caption: str = ""):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"
        try:
            with open(image_path, "rb") as photo:
                files = {"photo": photo}
                data = {"chat_id": self.chat_id, "caption": caption}
                requests.post(url, files=files, data=data)
        except Exception as e:
            logging.error(f"Telegram photo error: {e}")