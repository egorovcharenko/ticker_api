FROM python:alpine
ADD . .
WORKDIR code
RUN pip install -r requirements.txt
CMD ["python", "app.py"]