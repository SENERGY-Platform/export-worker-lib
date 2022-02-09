FROM python:3-alpine
WORKDIR /usr/src/app
COPY . .
RUN apk --no-cache add build-base librdkafka-dev librdkafka git && pip install --no-cache-dir -r requirements.txt && apk del git build-base librdkafka-dev
CMD [ "python", "-u", "worker.py"]