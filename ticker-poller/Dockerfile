FROM python:alpine
ADD . code
WORKDIR code
RUN pip install -r code/requirements.txt
CMD ["python", "code/app.py"]