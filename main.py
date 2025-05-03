import schedule
import time
import sys
from pipeline_runner import MainPipelineRunner
from utils.logger import setup_logger

'''
Как запускать
	•	Автозапуск по понедельникам в 09:00:
	    python main.py
	•	Принудительный запуск вручную:
		python main.py --now
'''

def schedule_weekly():
    runner = MainPipelineRunner()
    schedule.every().monday.at("09:00").do(runner.run)
    print("🕒 Планировщик активен...")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    setup_logger()

    if "--now" in sys.argv:
        print("🚨 Принудительный запуск пайплайна")
        runner = MainPipelineRunner()
        runner.run()
    else:
        schedule_weekly()