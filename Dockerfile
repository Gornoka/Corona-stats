FROM python:3.9
WORKDIR /usr/app
RUN apt-get update && apt-get upgrade -y && apt-get install cron -y
COPY crontab /etc/cron.d/simple-cron
COPY Statistic_downloader.py .
COPY configs/* ./configs/
COPY requirements.txt .

RUN python3 -m pip install -r requirements.txt
RUN touch /var/log/cron.log
CMD cron && tail -f /var/log/cron.log
#ENTRYPOINT python3 Statistic_downloader.py
