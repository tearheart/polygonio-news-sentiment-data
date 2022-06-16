FROM python:3.8-slim-bullseye

ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

CMD [ "python", "src/main.py"]
