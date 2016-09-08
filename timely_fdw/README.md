# Timely Postgres Foreign Data Wrapper

To use:

First time, pull the multicorn docker image:

```
docker pull danielfrg/multicorn
```

Run the multicorn docker image, mapping src directory into /src of the container

```
docker run -p 5432:5432 -v $(pwd)/src:/src multicorn
```

Connect to Postgres with your favorite tool:

pgadmin / sqlworkbench/J


