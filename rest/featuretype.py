# -*- coding: utf-8 -*-
# @Author: zhangbo
# @E-mail: xtfge_0915@163.com
# @Date:   2018-12-27 20:50:13
# @Last Modified by:   zhangbo
# @Last Modified time: 2019-01-09 04:41:19

from rest.tools import build_url, base_handler
from rest.engine import default_engine
import lxml.etree as ET
import geopandas as gpd
from sqlalchemy import *
from sqlalchemy.engine.url import URL
from rest.tools import epsg_from_crs,epsg_to_wkt,epsg_to_proj4
from shapely.geometry import Point
from rest.tools import Datastoretype
from rest.tools import PostgisORM
import re,os,sys,traceback
import logging
from rest.tools import walkAllDir
from rest.tools import create_ORM_url
from rest.tools import string_node,create_xml_node,bbox

FEATURETYPE_BODY_TEMPLATE='resources/feature.tpl.xml'

class Featuretype(base_handler):
    def __init__(self, engine,workspace=None,datastore=None,name=None):
        self.engine=engine
        self.workspace=workspace
        self.datastore=datastore
        self.name=name
        self._enabled='true'

    @property
    def url(self):
    	args=['workspaces',self.workspace.name]
    	if self.datastore:
    		args.append('datastores')
    		args.append(self.datastore.name)
    	args.append('featuretypes')
    	args.append(self.name)
    	return build_url(self.engine.service_url,args)
    def drop(self,recurse=False):
    	if self.name is None:
    		raise Exception('Its parameter "name" must be defined')
    	self.http_request(self.url+"?recurse=%s" % recurse,method='delete')
    	print('successfully drop featuretype "%s,%s".' % (self.workspace.name,self.name))
    @property
    def items(self):
    	pass
    @property
    def enabled(self):
    	return self._enabled
    @enabled.setter
    def enabled(self,value):
    	if value:
    		self._enabled='true'
    		self.set_enabled(self.enabled)
    	else:
    		self._enabled='false'
    		self.set_enabled(self.enabled)

    def set_enabled(self,value):
    	_body=ET.fromstring(self.body)
    	try:
    		_body.find('enabled').text=value
    	except:
    		_body.append(create_xml_node('enabled',text=value))
    	self.http_request(self.url,data=ET.tostring(_body),method='put')
    	print('updated successfully.')
    
    


class Featuretypes(base_handler):
    def __init__(self, engine,workspace,datastore=None):
        self.engine=engine
        self.workspace=workspace
        self.datastore=datastore

    @property
    def url(self):
    	args=['workspaces',self.workspace.name]
    	if self.datastore:
    		args.append('datastores')
    		args.append(self.datastore.name)
    	args.append('featuretypes')
    	# args.append(self.name)
    	# args.append('china_province')


    	return build_url(self.engine.service_url,args)
    
    
    def generate_feature_body(self,feature,datastore):
    	# root=ET.parse(FEATURETYPE_BODY_TEMPLATE).getroot()
    	_body=ET.Element('featureType')
    	_body.append(create_xml_node('name',text=feature.name))
    	_body.append(create_xml_node('nativeName',text=feature.nativename))
    	_body.append(create_xml_node('namespace'))
    	_body.find('namespace').append(create_xml_node('name',text=self.workspace.name))
    	namespace={'atom':'http://www.w3.org/2005/Atom'}
    	attr={'href':"http://localhost:8080/geoserver/rest/namespaces/%s.xml" % self.workspace, "rel":"alternate", \
    	"type":"application/xml"}
    	_body.find('namespace').append(create_xml_node('link',attr,namespace=namespace))
    	_body.append(create_xml_node('title',text=feature.title))
    	_body.append(create_xml_node('keywords'))
    	for key in feature.keywords:
    		_body.find('keywords').append(string_node(key))
    	
    	wkt=epsg_to_wkt(int(feature.crs.split(':')[1]))
    	_body.append(create_xml_node('nativeCRS',text=wkt))
    	_body.append(create_xml_node('srs',text=feature.crs))
    	_body.append(bbox('nativeBoundingBox',feature.nativebbox))
    	_body.find('nativeBoundingBox').append(create_xml_node('crs',\
    		{'class':"projected"},feature.crs))
    	_body.append(bbox('latLonBoundingBox',feature.latlonbbox))
    	_body.find('latLonBoundingBox').append(create_xml_node('crs',text=feature.latloncrs))
    	_body.append(create_xml_node('enabled',text=feature.enabled))
    	_body.append(create_xml_node('store',{'class':"dataStore"}))
    	_body.find('store').append(create_xml_node('name',text="%s:%s" %\
    		(self.workspace.name,datastore.name)))
    	storelink=ET.Element('{http://www.w3.org/2005/Atom}link',{"href":"http://\
    		localhost:8080/geoserver/rest/workspaces/%s/datastores/%s.xml" % \
    		(self.workspace.name,self.datastore.name),"rel":"alternate" ,"type":"application/xml"})
    	_body.find('store').append(storelink)
    	_body.append(create_xml_node('attributes'))

    	types=feature.data.dtypes
    	for attr in range(len(feature.fields)):
    		_name=feature.fields[attr]
    		if _name=='geometry':
	    		_name='the_geom'
    		_body.find('attributes').append(create_xml_node('attribute'))
    		_body.find('attributes/attribute[%d]' % (attr+1)).append(\
    			create_xml_node('name',text=_name))
    		_body.find('attributes/attribute[%d]' % (attr+1)).append(\
    			create_xml_node('minOccurs',text='0'))
    		_body.find('attributes/attribute[%d]' % (attr+1)).append(\
    			create_xml_node('maxOccurs',text='1'))
    		_body.find('attributes/attribute[%d]' % (attr+1)).append(
    			create_xml_node('nillable',text='true'))
    		_type,length=feature.data_type_mapping(feature.types[attr])
    		_body.find('attributes/attribute[%d]' % (attr+1)).append(\
    			create_xml_node('binding',text=_type))
			# print(feature.types[attr],feature.data_type_mapping(feature.types[attr]))
    		if datastore.type==Datastoretype.shapefile.value or datastore.type==Datastoretype.directory.value:
	    		if length:
	    			_body.find('attributes/attribute[%d]' % (attr+1)).append(\
	    				create_xml_node('length',text=length))
    		# print(str(feature.types[feature.fields.index(attr)])
    	# _body=ET.tostring(_body)
    	# ET.ElementTree(_body).write('resources/aaa.xml')


    	
    	return ET.tostring(_body)
    def publish(self,nativename,name=None,datastore=None,password=None,feature_setter_function=None):
    	self.add(nativename,name,datastore,password,feature_setter_function)

    def add(self,nativename,name=None,datastore=None,password=None,feature_setter_function=None):
    	if datastore is None and self.datastore is None:
    		raise Exception('datastore undefined.')
    	_datastore=datastore if datastore else self.datastore

    	if _datastore.type==Datastoretype.postgis.value and password is None:
    		raise Exception('Please provide password for postgis.')
    	if _datastore.type==Datastoretype.postgis.value:
    		url,schema=create_ORM_url(_datastore.connectionParameters)(password)
    		ormdb=PostgisORM(url=url)
	    	if nativename not in ormdb.get_table_list(schema):
	    		raise Exception('table "%s" no exist.' % nativename)
	    	data=gpd.read_postgis("select * from %s" % nativename,con=ormdb.engine)
	    	feat=FeatureInfo(nativename,data,name=name)
	    	# print(feat.fields)
	    	feat.fields=feat.fields[1:]
    	if _datastore.type==Datastoretype.shapefile.value:
	    	url=_datastore.connectionParameters['url']
	    	filepath=re.match('file://*(.*)',url).group(1) if re.match('file://*(.*)',url) else url
	    	if os.path.exists(filepath)==False:
	    		raise Exception('datastore "%s" has no "%s"' % (_datastore.name,nativename))
	    	data=gpd.read_file(filepath)
	    	if nativename is None:
	    		nativename=os.path.splitext(os.path.basename(filepath))[0]
	    	feat=FeatureInfo(nativename,data,name=name)

    	if _datastore.type==Datastoretype.directory.value:
	    	url=_datastore.connectionParameters['url']
	    	filepath=os.path.join(re.match('file:/*(/.*)',url).group(1),\
	    		"{0}.shp".format(nativename))
	    	# print(filepath)
	    	if os.path.exists(filepath)==False:
	    		raise Exception('datastore "%s" has no "%s"' % (_datastore.name,nativename))
	    	data=gpd.read_file(filepath)
	    	feat=FeatureInfo(nativename,data,name=name)
    	# if _datastore.enabled=="false":
	    # 	raise Exception("datastore '%s' is disabled" % _datastore.name)
	    
    	if feature_setter_function:
    		feat=feature_setter_function(feat)
    		if isinstance(feat,FeatureInfo)==False:
    			raise Exception('feature_setter_function must return Feature')
    	fs=Featuretypes(self.engine,self.workspace).items
    	if feat.name in fs:
    		raise Exception('featuretype "%s:%s" already exists.' % (self.workspace.name,feat.name))
    	feat_body=self.generate_feature_body(feat,_datastore)



    	print(self.url)# feature_body=self.generate_feature_body(feature)
    	self.http_request(self.url,method='post',data=feat_body)
    	print("successfully add featuretype '%s'." % feat.name)
    # def drop(self,recurse=False):
    # 	if self.name is None:
    # 		raise Exception('Its parameter "name" must be defined')
    # 	self.http_request(self.url+"?recurse=%s" % recurse,method='delete')
    # 	print('successfully drop featuretype "%s,%s".' % (self.workspace.name,self.name))
    def get_featuretype_body_template(self,savepath,datastoretype=Datastoretype.shapefile):
    	if re.match('.*(\.xml)$',savepath) is None:
    		savepath=savepath+".xml"
    	if datastoretype==Datastoretype.shapefile or datastoretype==Datastoretype.shapefile.value:
    		fp=open('resources/featuretype_shp.xml')
    	if datastoretype==Datastoretype.postgis or datastoretype==Datastoretype.postgis.value:
    		fp=open('resources/featuretype_postgis.xml')

    	writer=open(savepath,'w')
    	writer.write(fp.read())
    def __getitem__(self,key):
    	if isinstance(key,int):
    		return Featuretype(self.engine,workspace=self.workspace,\
    			datastore=self.datastore,name=self.items[key])
    	elif isinstance(key,str):
    		return Featuretype(self.engine,workspace=self.workspace,\
    			datastore=self.datastore,name=key)
    	else:
    		raise Exception('keyError')


class FeatureInfo():
    def __init__(self,nativename,featreader,name=None):
    	self.data=featreader
    	self._nativename=nativename
    	self._enabled='true'
    	if name:
    		self._name=name
        # if datastoretype==Datastoretype.shapefile:
        #     self.table=os.path.basename(filepath)
        #     self.data=gpd.read_file(filepath)
        # elif datastoretype==Datastoretype.directory:
        #     self.table=walkAllDir(filepath,".shp")
        #     self.data=[]
        #     for tb in self.table:
        #         self.data.append(gpd.read_file(tb))
        # elif datastoretype==Datastoretype.postgis:
            
        #     params=",".join(kwargs.keys())

        #     self.username=kwargs[re.match('.*(user[^,]*),??.*',params,re.IGNORECASE).group(1)] \
        #     if re.match('.*(user[^,]*),??.*',params,re.IGNORECASE)  else 'postgres'
        #     self.password=kwargs[re.match('.*(user[^,]*),??.*',rams,re.IGNORECASE).group(1)] \
        #     if re.match('.*(pass[^,]*),??.*',params,re.IGNORECASE) else 'postgres'
        #     self.port=kwargs[re.match('.*(user[^,]*),??.*',params,re.IGNORECASE).group(1)] \
        #     if re.match('.*(port[^,]*),??.*',params,re.IGNORECASE) else 5432
        #     self.host=kwargs[re.match('.*(user[^,]*),??.*',params,re.IGNORECASE).group(1)] \
        #     if re.match('.*(host[^,]*),??.*',params,re.IGNORECASE) else 'localhost'
        #     self.database=kwargs[re.match('.*((db[^,]*)|(database[^,]*)),??.*',params,re.IGNORECASE).group(1)] \
        #     if re.match('.*((db[^,]*)|(database[^,]*)),??.*',params,re.IGNORECASE) else 'postgres'

        #     url = 'postgresql://%s:%s@%s:%s/%s'% (self.username,self.password,self.host,self.port,self.database)
        #     print(url)
        #     postgis_orm=PostgisORM(url=url)
        #     self.table=table if table else postgis_orm.get_table_list()

        #     if isinstance(self.table,str):
        #         self.data=gpd.read_postgis("select * from %s" % self.table, postgis_orm.engine)
        #     if isinstance(self.table,list):
        #         self.data=[]
        #         for tb in self.table:
        #             self.data.append(gpd.read_postgis("select * from %s" % tb, postgis_orm.engine))

    @property
    def name(self):
    	if hasattr(self,'_name'):
    		return self._name
    	self._name=self.nativename
    	return self._name
    
    @name.setter
    def name(self,name):
        self._name=name


    @property
    def nativename(self):
        return self._nativename
    @property
    def keywords(self):
    	if hasattr(self,'_keywords'):
    		return self._keywords
    	else:
    		self._keywords=['features',self.name]
    		return self._keywords

    @keywords.setter
    def keywords(self,keywords):
    	if isinstance(keywords,str):
    		self._keywords=[]
    		self._keywords.append(keywords)
    	if isinstance(keywords,list):
    		self._keywords=keywords
    @property
    def enabled(self):
    	return self._enabled
    @enabled.setter
    def enabled(self,value):
    	if value:
    		self._enabled='true'
    	else:
    		self._enabled='false'
    


    @property  
    def title(self):
        if hasattr(self, '_title'):
            return self._title
        self._title=self.name
        return self._title
    @title.setter
    def title(self,title):
        self._title=title
    
    @property
    def keys(self):
        return self._keys
    @keys.setter
    def keys(self,*args):
        self._keys=list(args)
    @property
    def crs(self):
        self._crs=epsg_from_crs(self.data.crs)
        return "EPSG:%s" % self._crs
    # @crs.setter
    # def crs(self,crs=None,epsg=None):
    #     if crs:
    #         self._crs= "EPSG:%s" % epsg_from_crs(crs)
    #     else:
    #         self._crs="EPSG:%s" % epsg
    @property 
    def boundary(self):
        if hasattr(self, '_boundary'):
            return self._boundary
        self._boundary=self.data_boundary
        return self._boundary
    
    @boundary.setter
    def boundary(self,bounds=None,**kwargs):
        if bounds:
        	self._boundary=bounds
        else:
        	self._boundary=[kwargs[minx],kwargs[miny],kwargs[maxx],kwargs[maxy]]

    @property
    def data_boundary(self):
        # self.data=bounds
        minx=self.data.bounds.minx.min()
        miny=self.data.bounds.miny.min()
        maxx=self.data.bounds.maxx.max()
        maxy=self.data.bounds.maxy.max()
        return [minx,miny,maxx,maxy]
    @property
    def nativebbox(self):
        return self.boundary
    @property
    def latloncrs(self):
    	if hasattr(self,'_latloncrs'):
    		return self._latloncrs
    	self._latloncrs="EPSG:4326"
    	return self._latloncrs
    @latloncrs.setter
    def latloncrs(self,value):
    	self._latloncrs=value
    
    
    @property   
    def latlonbbox(self):
        to_crs=self.latloncrs
        to_crs=epsg_to_proj4(int(to_crs.split(':')[1]))

        crs=epsg_to_proj4(int(self.crs.split(':')[1]))
        
        minp=gpd.GeoSeries(Point(self.boundary[0],self.boundary[1]),crs=crs).to_crs(to_crs)
        maxp=gpd.GeoSeries(Point(self.boundary[2],self.boundary[3]),crs=crs).to_crs(to_crs)
        return [float(minp.x),float(minp.y),float(maxp.x),float(maxp.y)]
    
    @property
    def attributes(self):
        self._attributes=self.data[self.fields]
        return self._attributes
    @property
    def fields(self):
        if hasattr(self,'_fields'):
        	return self._fields
        self._fields=self.data.columns.tolist()
        return self._fields
    @fields.setter
    def fields(self,fields):
    	self._fields=fields
    
    @property
    def geom(self):
        return self.data.geom
    @property
    def geom_type(self):
        return self.data.geom_type[0]
    
    @property
    def types(self):
    	self._types=self.attributes.dtypes
    	self._types[-1]=self.geom_type
    	return self._types
    
        

    def data_type_mapping(self,datatype):
        datatype=str(datatype)
        if re.match('str.*',datatype,re.IGNORECASE):
            return ('java.lang.String','100')
        elif re.match('object',datatype,re.IGNORECASE):
            return ('java.lang.String','100')
        elif re.match('int.*',datatype,re.IGNORECASE):
            return ('java.lang.Integer','9')#Integer
        elif re.match('float.*',datatype,re.IGNORECASE):
            return ('java.math.BigDecimal','29')
        elif re.match('.*Polygon.*',datatype,re.IGNORECASE):
            return ('org.locationtech.jts.geom.MultiPolygon',None)
        elif re.match('.*Point',datatype,re.IGNORECASE):
            return ('org.locationtech.jts.geom.Point',None)
        elif re.match('.*LineString',datatype,re.IGNORECASE):
            return ('org.locationtech.jts.geom.MultiLineString',None)
        
        else:
            return ('java.lang.String','100')

    # def __getitem__(self,item):



   
