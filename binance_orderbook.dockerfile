FROM python:3.11

RUN apt-get update

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install -U tornado==6.2

RUN mkdir templates
COPY templates/binance_orderbook.html templates
COPY binance_orderbook.py .
EXPOSE 5000

CMD ["python","binance_orderbook.py"]