FROM python:3.6

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

RUN mkdir /code
RUN mkdir /code/log

WORKDIR /code

COPY requirements.txt /code/


RUN pip install --upgrade pip wheel
RUN pip install -r requirements.txt

EXPOSE 8008

COPY src/ /code/
VOLUME ./src /code/

RUN python /code/manage.py migrate

CMD ["python", "/code/manage.py", "runserver", "0.0.0.0:8000"]