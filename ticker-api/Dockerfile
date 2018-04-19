FROM python:alpine

ADD . .

RUN pip3 install -U pip \
    && pip3 install -r requirements.txt

WORKDIR /code/

CMD ["python", "app.py"]
