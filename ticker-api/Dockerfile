FROM python:3.6

ADD . .

RUN pip3 install -U pip \
    && pip3 install -r requirements.txt

WORKDIR /code/

CMD ["apistar", "run", "--host", "0.0.0.0", "--port", "80"]
