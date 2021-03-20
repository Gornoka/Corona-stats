FROM python:3.9
WORKDIR /usr/app
COPY Statistic_downloader.py .
COPY configs/* ./configs/
COPY requirements.txt .
RUN python3 -m pip install -r requirements.txt
ENTRYPOINT python3 Statistic_downloader.py