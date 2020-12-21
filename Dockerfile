# For more information, please refer to https://aka.ms/vscode-docker-python
FROM cremarco/mantis4

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

ENV PORT=80
ENV THREADS=16
ENV LAMAPI=True