# -*- coding: utf-8 -*-
# @Author: zhangbo
# @E-mail: xtfge_0915@163.com
# @Date:   2018-12-22 19:07:55
# @Last Modified by:   zhangbo
# @Last Modified time: 2019-01-06 00:13:30

import requests, sys, logging, json
import re, os, sys
import pyproj
import fiona.crs
from osgeo.osr import SpatialReference
from enum import Enum, unique
import traceback
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.url import URL
import traceback
import pandas as pd
import lxml.etree as ET
from shapely.geometry import Point
import geopandas as gpd

CHINA_LATLON_BOUNDARY = [73.498962, 3.833843, 135.087387, 53.558498]
CHINA_EPSG3857_BOUNDARY = [8181867.023674167, 427100.28419312113, 15037859.133433886, 7086980.581736245]


def walkAllDir(rootdir, filter="*"):
    '''
     walk all dir .including rootdir and it's all children dir
    :param rootdir:catalog will be walked
    :param filter:file extent name that you want to filter,for example .txt,.tif.xls ect.
    :return:list of files
    '''
    names = []
    if os.path.exists(rootdir) == False:
        print("root dir no exist!!")
        sys.exit(1)
    for parent, dirs, files in os.walk(rootdir):
        for file in files:
            name, ext = os.path.splitext(file)
            if ext == filter or "*" == filter:
                names.append(os.path.join(parent, file))
    return names


def build_url(base, params, query=None):
    """
    Create a URL from a list of path segments and an optional dict of query
    parameters.
    """
    if isinstance(params, str):
        return url_join(base, params)
    elif isinstance(params, list):
        params = [p for p in params if p is not None]
        params = "/".join(params)
        return url_join(base, params)
    else:
        raise Exception('Type Error:params must be a string or string list')


def explicit_crs_from_epsg(crs=None, epsg=None):
    """
    Gets full/explicit CRS from EPSG code provided.
    Parameters
    ----------
    crs : dict or string, default None
        An existing crs dict or Proj string with the 'init' key specifying an EPSG code
    epsg : string or int, default None
       The EPSG code to lookup
    """
    if epsg is None and crs is not None:
        epsg = epsg_from_crs(crs)
    if epsg is None:
        raise ValueError('No epsg code provided or epsg code could not be identified from the provided crs.')

    _crs = re.search('\n<{}>\s*(.+?)\s*<>'.format(epsg), get_epsg_file_contents())
    if _crs is None:
        raise ValueError('EPSG code "{}" not found.'.format(epsg))
    _crs = fiona.crs.from_string(_crs.group(1))
    # preserve the epsg code for future reference
    _crs['init'] = 'epsg:{}'.format(epsg)
    return _crs


def epsg_from_crs(crs):
    """
    Returns an epsg code from a crs dict or Proj string.
    Parameters
    ----------
    crs : dict or string, default None
        A crs dict or Proj string
    """
    if crs is None:
        raise ValueError('No crs provided.')
    if isinstance(crs, str):
        crs = fiona.crs.from_string(crs)
    if not crs:
        raise ValueError('Empty or invalid crs provided')
    if 'init' in crs and crs['init'].lower().startswith('epsg:'):
        return int(crs['init'].split(':')[1])


def get_epsg_file_contents():
    with open(os.path.join(pyproj.pyproj_datadir, 'epsg')) as f:
        return f.read()


def epsg_to_wkt(epsg):
    sr = SpatialReference()
    sr.ImportFromEPSG(epsg)
    wkt = sr.ExportToWkt()
    if wkt == '':
        msg = requests.get('http://epsg.io/%d' % epsg)
        if msg.status_code == 200:
            txt = msg.text.replace('\n', '')
            wkt = re.match('.*<pre id="s_geoserver_text">\d*=(.*?)</pre>.*', txt).group(1)
    return wkt


def epsg_to_proj4(epsg):
    sr = SpatialReference()
    sr.ImportFromEPSG(epsg)
    proj4 = sr.ExportToProj4()
    if proj4 == '':
        msg = requests.get('http://epsg.io/%d' % epsg)
        if msg.status_code == 200:
            txt = msg.text.replace('\n', '')
            proj4 = re.match('.*<pre id="s_proj4_text">(.*?)</pre>.*', txt).group(1)

    return proj4


def project_transform(point, crs_epsg=4326, to_crs_epsg=3857):
    p = gpd.GeoSeries(point, crs=epsg_to_proj4(crs_epsg))
    new_p = p.to_crs(epsg=to_crs_epsg)
    return Point(new_p.x, new_p.y)


def url_join(*url):
    url = [u.strip('/') for u in url]
    return "/".join(url)


def create_ORM_url(connectionParameters):
    def URL(password):
        driver = 'postgresql' if connectionParameters['dbtype'].lower() == 'postgis' else connectionParameters['dbtype']
        username = connectionParameters['user']
        host = connectionParameters['host']
        port = connectionParameters['port']
        database = connectionParameters['database']
        schema = connectionParameters['schema']
        url = '%s://%s:%s@%s:%s/%s' % (driver, username, password, host, port, database)
        return (url, schema)

    return URL


def parameters_check(argsdic=None, **kwargs):
    _argdic = kwargs
    if argsdic:
        _argsdic = dict(argsdic, **kwargs)
    args = _argdic.keys()
    for arg in args:
        if _argdic[arg] is None:
            raise Exception('parameter "%s" must be assigned.' % arg)


def bbox(name, box):
    root = ET.Element(name)
    minx = ET.Element('minx')
    miny = ET.Element('miny')
    maxx = ET.Element('maxx')
    maxy = ET.Element('maxy')
    [minx.text, miny.text, maxx.text, maxy.text] = [str(x) for x in box]
    root.append(minx)
    root.append(maxx)
    root.append(miny)
    root.append(maxy)
    return root


def affine(affine):
    affine = [str(x) for x in affine]
    root = ET.Element('transform')
    root.append(create_xml_node('scaleX', text=affine[0]))
    root.append(create_xml_node('scaleY', text=affine[4]))
    root.append(create_xml_node('shearX', text=affine[1]))
    root.append(create_xml_node('shearY', text=affine[3]))
    root.append(create_xml_node('translateX', text=affine[2]))
    root.append(create_xml_node('translateY', text=affine[5]))
    return root


def string_node(text):
    node = ET.Element('string')
    node.text = text
    return node


def entry_key(key, value):
    return create_xml_node('entry', {'key': key}, value)


def xml_text(node, tags, value=None):
    if value:
        return node.find(tags).text
    else:
        node.find(tags).text = value


def create_xml_node(tags, attri=None, text=None, namespace=None):
    if namespace:
        for key in namespace.keys():
            ET.register_namespace(key, namespace[key])
            _node = ET.Element("{%s}%s" % (namespace[key], tags), attri)
    else:
        _node = ET.Element(tags, attri)
    if text:
        _node.text = text
    # print(ET.tostring(_node))
    return _node


class PostgisORM:
    def __init__(self, url=None, driver='postgresql', user='postgres', password='postgres', dbname=None,
                 host='localhost', port=5432, echo=False):
        try:
            if url:
                self.url = url
            else:
                self.url = URL(drivername=driver, username=user, password=password, database=dbname, host=host,
                               port=port)
            self.engine = create_engine(self.url, echo=echo)  # 创建引擎
            self.conn = self.engine.connect()  # 连接引擎
            self.metadata = MetaData(self.engine)  # 绑定引擎
        # self.base=declarative_base()
        except:
            traceback.print_exc()

    def get_database_list(self, *cols):
        colnames = ",".join(["datname", "user", "pg_encoding_to_char(encoding) as encoding"] + list(cols))
        sqlstr = "select %s from pg_database WHERE datistemplate = false" % colnames
        return self.query(sqlstr)

    def get_table_list(self, schema):
        sqlstr = "SELECT f_table_name FROM geometry_columns WHERE f_table_schema = '%s'" % schema
        return self.query(sqlstr)['f_table_name'].tolist()

    def query(self, sqlstr, columns=None):
        try:
            return pd.read_sql(sqlstr, con=self.engine, columns=columns)
        except:
            traceback.print_exc()


class Datastoretype(Enum):
    shapefile = 'Shapefile'
    postgis = 'PostGIS'
    directory = 'Directory of spatial files (shapefiles)'
    geopackage = 'GeoPackage'
    postgis_jndi = 'PostGIS (JNDI)'
    properties = 'Properties'
    web_feature_server = 'Web Feature Server (NG)'


class Coveragestoretype(Enum):
    arcgrid = "ArcGrid"
    geoPackage_mosaic = 'GeoPackage (mosaic)'
    geotiff = 'GeoTIFF'
    image_mosaic = 'ImageMosaic'
    image_mosaic_JDBC = 'ImageMosaicJDBC'
    world_image = 'WorldImage'


class StyleType(Enum):
    sld = 'application/vnd.ogc.sld+xml'
    sld_1_1 = 'application/vnd.ogc.se+xml'
    css = 'application/vnd.geoserver.geocss+css'
    ysld = 'application/vnd.geoserver.ysld+yaml'
    mb = 'application/vnd.geoserver.mbstyle+json'


class base_handler:
    def __init__(self, engine):
        self.engine = engine

    def http_request(self, url, data=None, method='get',
                     headers={'Accept': 'application/xml', 'content-type': 'application/xml'}):
        request_method = getattr(requests, method)
        rst = request_method(url, headers=headers, data=data, auth=(self.engine.user, self.engine.password))
        if rst.status_code // 100 != 2:  ##request failed
            logging.error('http request error. status code:%d,%s' % (rst.status_code, rst.reason))
            sys.exit()

        return rst

    @property
    def items(self):
        # print(self.url)
        _body = self.http_request(self.url, headers={'Accept': 'application/xml'}).text
        root = ET.fromstring(_body)
        items = root.findall('.//name')
        self._items = [item.text for item in items]

        return self._items

    def check_name(self, name):
        if re.match('^[\u4e00-\u9fa5_a-zA-Z0-9]+$', name) is None:
            logging.error('"name" contains illegal characters.')
            sys.exit()

    @property
    def body(self):
        return self.http_request(self.url).text

# def __str__(self):
# 	super().__str__()
# 	if 'name' not in dir(self):
# 		self.name=""
# 	return "%s@% s" % (self.name,self.url)
# def __repr__(self):
# 	return "%s@% s" % (self.name,self.url)


class dic_to_obj:
    def __init__(self, map):
        self.map = map

    def __getattr__(self, name):
        v = self.map[name]
        if (v, (dict)):
            return dic_to_obj(v)
        if isinstance(v, (list)):
            r = []
            for i in v:
                r.append(dic_to_obj(i))
            return r
        else:
            return self.map[name];


if __name__ == '__main__':
    a = {'name': {'fisrt': 'zhang', 'last': 'san'}, 'age': 18}
    b = dic_to_obj(a)
    print(Datastoretype.shapefile.value)
