FROM ubuntu:14.04

ENV PYTHONPATH="/code:/code/venv/lib/python3.5/site-packages"

ADD . /code

ENTRYPOINT ["python3", "/code/web/app.py"]