# MOBIKE API

18124722 ZhongLeXing  18124666 ZhuYiBing

# 1.INSTALLATION

1. move the weibo.csv, mobike.csv in mobike_volume
2. enter the docker-configuration to setup the cluster

```
$cd docker-configuration
$sudo docker-compose up
```



# 2.PREPROCESSING

Open 127.0.0.1:8899 and log in to jupyter lab token to view through logs jupyterlab

Run mobike.ipynb every step ,then write data to mysql databases



# 3.TEST

test our API with testapi.ipynb

# 4.CHANGE

change functionalities of API with api.py