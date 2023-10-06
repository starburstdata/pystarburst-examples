FROM python:3.11

COPY requirements.txt /

RUN pip install -r requirements.txt

COPY . /app

WORKDIR /app

EXPOSE 7860

CMD ["python", "-u", "app.py"]