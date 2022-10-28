## GeoServer-rest-python的总体结构
其结构与GeoServer相对应，最顶层是Catlog目录类，Catlog下有workspace(s),datastore(s),coveragestore(s),layer(s),style(s),layer下还有featuretype和coverage，需要说明的是catlog下的每个类都有两种，比如workspace和workspaces,前者指的是一个具体的worksapce，后者指是所有workspace的集合，其它同理。
## 安装依赖库
```
geopandas
rasterio
```
## 安装
```
python setup.py instal
```
