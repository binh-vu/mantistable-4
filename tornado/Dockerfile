FROM python:3.7-slim

# The enviroment variable ensures that the python output is set straight
# to the terminal with out buffering it first
ENV PYTHONUNBUFFERED 1
ENV TERM=xterm

# Create root directory for our project in the container
RUN mkdir /app
COPY ./requirements.txt /app
COPY ./server.py /app

# Set the working directory
WORKDIR /app

# Install python requiremensts
RUN pip install -r requirements.txt  --no-cache-dir
