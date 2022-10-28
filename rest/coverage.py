# -*- coding: utf-8 -*-
# @Author: zhangbo
# @E-mail: xtfge_0915@163.com
# @Date:   2019-01-04 20:23:27
# @Last Modified by:   zhangbo
# @Last Modified time: 2019-01-09 04:42:11

from rest.tools import build_url,base_handler
from rest.workspace import Workspace
from rest.store import Coveragestore
from rest.tools import epsg_to_wkt,epsg_to_proj4,epsg_from_crs,epsg_to_wkt
from  rasterio import open as openraster
import lxml.etree as ET
from rest.tools import Coveragestoretype,parameters_check,xml_text,create_xml_node
import os,re
from rest.tools import bbox,affine,string_node,entry_key
import geopandas as gpd
from shapely.geometry import Point



COVERAGE_BODY_TEMPLATE='resources/coverage.tpl.xml'

class Coverages(base_handler):
	def __init__(self,engine,workspace=None,coveragestore=None):
		self.engine=engine
		if isinstance(workspace,str):
			self.workspace=Workspace(self.engine,workspace)
		elif isinstance(workspace,Workspace):
			self.workspace=workspace
		else:
			raise Exception('workspace define error.')
		if isinstance(coveragestore,str):
			self.coveragestore=Coveragestore(self.engine,self.workspace,coveragestore)
		elif isinstance(coveragestore,Coveragestore):
			self.coveragestore=coveragestore
		else:
			self.coveragestore=None


	@property
	def url(self):
		_args=['workspaces',self.workspace.name]
		if self.coveragestore:
			_args.append('coveragestores')
			_args.append(self.coveragestore.name)
		_args.append('coverages')
		_url=build_url(self.engine.service_url,_args)
		return _url
	def generate_coverage_body(self,coveragestore,info,coveragestoretype=Coveragestoretype.geotiff):
		_body=ET.Element('coverage')
		_body.append(create_xml_node('name',text=info.name))
		_body.append(create_xml_node('nativeName',text=info.nativename))
		_body.append(create_xml_node('namespace'))
		_body.find('namespace').append(create_xml_node('name',text=self.workspace.name))
		wslink={"rel":"alternate","href":"%s.xml" % self.workspace.url,\
		"type":"application/xml"}
		namespace={'atom':'http://www.w3.org/2005/Atom'}
		_body.find('namespace').append(create_xml_node('link',wslink,namespace=namespace))
		_body.append(create_xml_node('title',text=info.title))
		_body.append(create_xml_node('description',text=info.description))
		_body.append(create_xml_node('abstract',text=info.abstract))
		_body.append(create_xml_node('keywords'))
		for key in info.keywords:
			_body.find('keywords').append(create_xml_node('string',text=key))
		wkt=epsg_to_wkt(info._crs)
		_body.append(create_xml_node('nativeCRS',{'class':"projected"},wkt))
		_body.append(create_xml_node('srs',text=info.crs))
		_body.append(bbox('nativeBoundingBox',info.nativebbox))
		_body.find('nativeBoundingBox').append(create_xml_node(\
			'crs',{'class':"projected"},info.crs))
		_body.append(bbox('latLonBoundingBox',info.latlonbbox))
		_body.find('latLonBoundingBox').append(create_xml_node('crs',text=info.latloncrs))
		_body.append(create_xml_node('projectionPolicy',text=info.projectionPolicy))
		_body.append(create_xml_node('enabled',text=info.enabled))
		_body.append(create_xml_node('store',{'class':'coverageStore'}))
		_body.find('store').append(create_xml_node('name',text="%s:%s" % \
			(self.workspace.name,coveragestore.name)))
		slink={"rel":"alternate","href":"%s.xml" % coveragestore.url,\
		"type":"application/xml"}
		_body.find('store').append(create_xml_node('link',slink,namespace=namespace))
		_body.append(create_xml_node('nativeFormat',text=coveragestoretype.value))
		_body.append(create_xml_node('grid',{'dimension':"2"}))
		_body.find('grid').append(create_xml_node('range'))
		_body.find('grid/range').append(create_xml_node('low',text="0 0"))
		_body.find('grid/range').append(create_xml_node('high',text="%d %d" \
			% (info.width,info.height)))
		_body.find('grid').append(affine(info.affine))
		_body.find('grid').append(create_xml_node('crs',text=info.crs))
		formats=("GIF","PNG","JPEG","TIFF","GEOTIFF","ImageMosaicJDBC",\
		"GeoPackage (mosaic)","ArcGrid","ImageMosaic")
		_body.append(create_xml_node('supportedFormats'))
		for f in formats:
			_body.find('supportedFormats').append(string_node(f))
		_body.append(create_xml_node('interpolationMethods'))
		intermethods=['nearest neighbor','bilinear','bicubic']
		for inter in intermethods:
			_body.find('interpolationMethods').append(string_node(inter))
		_body.append(create_xml_node('defaultInterpolationMethod',text='nearest neighbor'))
		_body.append(create_xml_node('dimensions'))
		_body.find('dimensions').append(create_xml_node('coverageDimension'))
		_body.find('dimensions/coverageDimension').append(create_xml_node('name',text='GRAY_INDEX'))
		_body.find('dimensions/coverageDimension').append(create_xml_node(\
			"description",text='GridSampleDimension[-Infinity,Infinity]'))
		_body.find('dimensions/coverageDimension').append(create_xml_node('range'))
		_body.find('dimensions/coverageDimension/range').append(create_xml_node(\
			'min',text='-inf'))
		_body.find('dimensions/coverageDimension/range').append(create_xml_node(\
			'max',text='inf'))
		if info.nodata:
			_body.find('dimensions/coverageDimension').append(create_xml_node('nullValues'))
			_body.find('dimensions/coverageDimension/nullValues').append(create_xml_node(\
				'double',text=str(info.nodata)))
		_body.find('dimensions/coverageDimension').append(create_xml_node('dimensionType'))
		_body.find('dimensions/coverageDimension/dimensionType').append(create_xml_node(\
			'name',text=info.dtype))
		_body.append(create_xml_node('requestSRS'))
		_body.find('requestSRS').append(string_node(info.crs))
		_body.append(create_xml_node('responseSRS'))
		_body.find('responseSRS').append(string_node(info.crs))
		_body.append(create_xml_node('parameters'))
		_body.find('parameters').append(create_xml_node('entry'))
		_body.find('parameters/entry').append(string_node('InputTransparentColor'))
		_body.find('parameters/entry').append(create_xml_node('null'))
		_body.find('parameters').append(create_xml_node('entry'))
		_body.findall('parameters/entry')[-1].append(string_node('SUGGESTED_TILE_SIZE'))
		_body.findall('parameters/entry')[-1].append(string_node('512,512'))
		_body.append(create_xml_node('nativeCoverageName',text=info.nativename))




		# print(ET.tostring(_body))
		return ET.tostring(_body)

	def publish(self,nativename=None,coveragename=None,coveragestore=None,\
		coveragestoretype=Coveragestoretype.geotiff):
		self.add(nativename,overagename,coveragestore,coveragestoretype)




	def add(self,nativename=None,coveragename=None,coveragestore=None,\
		coveragestoretype=Coveragestoretype.geotiff,coverage_setter_function=None):
		_storetype=coveragestore.type if isinstance(coveragestore,Coveragestore) else coveragestoretype.value
		if _storetype==Coveragestoretype.geotiff.value:
			_coveragestore=coveragestore if coveragestore else self.coveragestore
			parameters_check(coveragestore=_coveragestore)
			filename=ET.fromstring(coveragestore.body).find('url').text
			reader=openraster(filename)
			_nativename=os.path.splitext(os.path.basename(filename))[0]

		coverageinfo=CoverageInfo(_nativename,reader)
		if coveragename:
			coverageinfo.name=coveragename

		_body=self.generate_coverage_body(coveragestore,coverageinfo)
		if coverage_setter_function:
			_body=coverage_setter_function(_body)
		self.http_request(self.url,data=_body,method='post')
		print('successfully add a coverage.')

	def __getitem__(self,value):
		if isinstance(value,int):
			return Coverage(self.engine,self,workspace,self.items[value],self.coveragestore)
		elif isinstance(value,str):
			return Coverage(self.engine,self.workspace,value,self.coveragestore)
		else:
			raise Exception('KeyError')



class Coverage(base_handler):
	def __init__(self,engine,workspace=None,name=None,coveragestore=None):
		self.engine=engine
		if isinstance(workspace,str):
			self.workspace=Workspace(self.engine,workspace)
		elif isinstance(workspace,Workspace):
			self.workspace=workspace
		else:
			raise Exception('workspace define error.')
		if name is None:
			raise Exception('parameter "name" must be assigned.')
		self.name=name
		if isinstance(coveragestore,str):
			self.coveragestore=Coveragestore(self.engine,self.workspace,coveragestore)
		elif isinstance(coveragestore,Coveragestore):
			self.coveragestore=coveragestore
		else:
			self.coveragestore=None
	@property
	def url(self):
		_args=['workspaces',self.workspace.name]
		if self.coveragestore:
			_args.append('coveragestores')
			_args.append(self.coveragestore.name)
		_args.append('coverages')
		_args.append(self.name)
		_url=build_url(self.engine.service_url,_args)
		return _url
	@property
	def items(self):
		pass
	
	def drop(self,recurse=False):
		_url=self.url+"?recurse=%s" % recurse
		self.http_request(_url,method='delete')
		print('successfully drop coverage "%s"' % self.name)

	




class CoverageInfo(object):
	def __init__(self,nativename,rasterreader):
		self._crs=epsg_from_crs(rasterreader.crs)
		self._affine=rasterreader.affine
		self._nodata=rasterreader.nodata
		self._bounds=list(rasterreader.bounds)
		self._meta=rasterreader.meta
		self._nativename=nativename
		self._enabled='true'

	@property
	def nativename(self):
		return self._nativename
	@property
	def name(self):
		if hasattr(self,'_name'):
			return self._name 
		self._name=self.nativename
		return self._name
	@name.setter
	def name(self,value):
		self._name=value
	@property
	def title(self):
		if hasattr(self,'_title'):
			return self._title
		self._title=self.name
		return self._title
	@title.setter
	def title(self,value):
		self._title=value
	@property
	def crs(self):
		return "EPSG:%s" % self._crs
	@property
	def nodata(self):
		return self._nodata
	@property
	def bounds(self):
		return self._bounds
	@bounds.setter
	def bounds(self,value):
		self._bounds=value
	@property
	def nativebbox(self):
		return self.bounds
	@property
	def latloncrs(self):
		return "EPSG:4326"
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
	def latlonbbox(self):
	    to_crs=self.latloncrs
	    to_crs=epsg_to_proj4(int(to_crs.split(':')[1]))
	    crs=epsg_to_proj4(int(self._crs))
	    minp=gpd.GeoSeries(Point(self.nativebbox[0],self.nativebbox[1]),crs=crs).to_crs(to_crs)
	    maxp=gpd.GeoSeries(Point(self.nativebbox[2],self.nativebbox[3]),crs=crs).to_crs(to_crs)
	    return [float(minp.x),float(minp.y),float(maxp.x),float(maxp.y)]
	@property
	def projectionPolicy(self):
		if hasattr(self,'_projectionPolicy'):
			return self._projectionPolicy
		self._projectionPolicy='REPROJECT_TO_DECLARED'

		return self._projectionPolicy
	@projectionPolicy.setter
	def projectionPolicy(self,value):
		if value in ['REPROJECT_TO_DECLARED','FORCE_DECLARED','KEEP_NATIVE']:
			self._projectionPolicy=value
		else:
			raise Exception("unsupported projectionPolicy '%s'"% value)
	

	
	@property
	def affine(self):
		return self._affine

	@property
	def dtype(self):
		return self.dimension_type_mapping(self._meta['dtype'])
	@property
	def description(self):
		if hasattr(self,'_description'):
			return self._description
		self._description=self.name
		return self._description
	@description.setter
	def description(self,value):
		self._description=value
	@property
	def abstract(self):
		if hasattr(self,'_abstract'):
			return self._abstract
		self._abstract=self.name
		return self._abstract
	@property
	def keywords(self):
		if hasattr(self,'_keywords'):
			return self._keywords
		self._keywords=[self.nativename,self._meta['driver']]
		return self._keywords
	def add_keyword(self,value):
		self.keywords.append(value)
	@keywords.setter
	def keywords(self,keywords):
		if isinstance(keywords,str):
			self._keywords=[keywords]
		if isinstance(keywords,list):
			self._keywords=keywords
	@property
	def width(self):
		return self._meta['width']
	@property
	def height(self):
		return self._meta['height']
	
	
	
	
	


	def dimension_type_mapping(self,type):
		_tpye=[]
		if type[0].lower()=='u':
			_tpye.append('UNSIGNED')
		else:
			_tpye.append('SIGNED')

		_tpye.append('%sBITS' % re.match('\D*(\d*)\D*',type).group(1))
		# print("_".join(_tpye))
		return "_".join(_tpye)
	
	
	

	

