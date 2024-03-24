FROM python:3.10-bullseye

WORKDIR /src

COPY ./requirements.txt /src/requirements.txt

COPY ./app /src/app

RUN pip install --no-cache-dir --upgrade -r /src/requirements.txt

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
