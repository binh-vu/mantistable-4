FROM node:14

# Create root directory for our project
RUN mkdir /reactapp

ENV PATH /reactapp/node_modules/.bin:$PATH

# install app dependencies
COPY package.json ./reactapp
COPY package-lock.json ./reactapp
RUN npm install --silent
RUN npm install react-scripts@3.4.1 -g --silent

# add app
COPY . ./reactapp

WORKDIR /reactapp
