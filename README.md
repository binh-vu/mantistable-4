# MantisTable
MantisTable is a tool for Semantic Table Interpretation. The tool needs a running [LamAPI](https://bitbucket.org/disco_unimib/lamapi) endpoint.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine.

### Prerequisites and installation

The tool requires to have Docker installed and Docker-Compose. It also requires Node/NPM for GUI dependencies.

To install the requirements on Linux run:
```
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USERNAME
sudo wget -O  /usr/local/bin/docker-compose https://github.com/docker/compose/releases/download/1.25.0/docker-compose-Linux-x86_64
sudo chmod +x /usr/local/bin/docker-compose
```

While on [Windows](https://hub.docker.com/editions/community/docker-ce-desktop-windows) or [Mac](https://hub.docker.com/editions/community/docker-ce-desktop-mac) get Docker Desktop CE.

To install NodeJS on any debian based distribution:
```
curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash –
sudo apt-get install -y node-js
```
Or checkout the [Download page](https://nodejs.org/it/download/) for Windows or Mac.
ATTENTION: volumes are quite slow on Windows, we suggest to use WSL2 and clone this repo inside the Linux home directory running compose from there.
Now clone this repository.
You can checkout the tag MantisTableSE4 to get the version used during SemTab2020. We use the development branch actively and from time to time it may be broken while master branch should be always runnable.

Copy the `.env-example` file in a `.env` file.

PORT is the port where the tool will start, THREADS is the number of threads that will be used concurrently (suggested as the number of CPU cores you have available), LAMAPI is set to True if you want to use a local endpoint or False to use our demo remote endpoint (network overhead is much relevant, this is not suggested).

Now install the npm dependencies:
```
cd django && npm install
cd ../frontend && npm install
cd ..
```

And finally start MantisTable SE with docker-compose:
```
docker-compose -f docker-compose-prod.yml up
```

### App settings
If you can't reach the frontend dashboard you probably have to add your host to the `settings.py` file under ALLOWED_HOSTS.
Check line 50. In this file you can also change username and password for the dashboard.

The main interface is currently under development, you can reach a small admin dashboard on `http://localhost:PORT/dashboard/`.
The default username is `admin` and the default password is `mantis4`.



## License

This project is licensed under the Apache License - see the [LICENSE](LICENSE) file for details


