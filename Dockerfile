FROM python:3.9
WORKDIR /usr/app
# install deps for cronjob
RUN apt-get update && apt-get upgrade -y && apt-get install cron -y && apt-get install dos2unix
#create crontab ( pulled ahead of python related stuff, since this is unlikely to change much)
COPY crontab /etc/cron.d/simple-cron
RUN dos2unix /etc/cron.d/simple-cron
RUN chmod 644 /etc/cron.d/simple-cron
RUN crontab /etc/cron.d/simple-cron
# install scripts
COPY Statistic_downloader.py .
COPY configs/* ./configs/
COPY requirements.txt .

RUN python3 -m pip install -r requirements.txt
# touch logfile for tail
RUN touch /var/log/cron.log

CMD cron && tail -f /var/log/cron.log
#ENTRYPOINT python3 Statistic_downloader.py
