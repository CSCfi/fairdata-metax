FROM python:3.8-alpine

RUN pip install Sphinx sphinx-autobuild sphinx-rtd-theme
RUN mkdir -p /sphinx/build

WORKDIR /sphinx

EXPOSE 8000

CMD ["sphinx-autobuild", "--host", "0.0.0.0", "-E", "-j", "auto", "/sphinx/", "/sphinx/build/"]
