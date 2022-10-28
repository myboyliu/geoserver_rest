# -*- coding: utf-8 -*-
# @Author: zhangbo
# @E-mail: xtfge_0915@163.com
# @Date:   2018-12-23 14:18:01
# @Last Modified by:   zhangbo
# @Last Modified time: 2019-01-09 04:45:33

from rest.tools import base_handler,build_url
from rest.engine import Engine,default_engine
from rest.layer import Layer
import json,logging,re,os,requests
import lxml.etree as ET
from rest.tools import Datastoretype,Coveragestoretype
from rest.tools import parameters_check
from rest.tools import create_xml_node,string_node,entry_key
DATASTORE_TEMPLATE="./resources/datastore.xml"
SHAPFILE_DEFAULT_PARAMETERS={
	"charset":"GBK",
	"filetype":"shapefile",
	"url":"*.shp",
	"cache_and_reuse_memory_maps":'true',
	"create_spatial_index":"true",
	"memory_mapped_buffer":"false",
	"timezone":"Pacific Standard Time"
}
POSTGIS_DEFAULT_PARAMETERS={
	"host":"localhost",
	"user":"postgres",
	"passwd":"postgres",
	"database":"postgres",
	"Connection timeout":"20",
	"port":"5432",
	"validate_connections":"true",
	"Primary_key_metadata_table":"",
	"Support_on_the_fly_geometry_simplification":"true",
	"create_database":"false",
	"create_database params":"",
	"dbtype":"postgis",
	"Batch_insert size":"1",
	"Namespace":"",
	"Max_connection_idle_time":"300",
	"Session_startup_SQL":"",
	"Expose_primary_keys":"false",
	"min_connections":"1",
	"Max_open_prepared_statements":"50",
	"Callback_factory":"",
	"encode_functions":"false",
	"Evictor_tests_per_run":"3",
	"Loose_bbox":"true",
	"Evictor_run_periodicity":"300",
	"Estimated_extends":"true",
	"fetch_size":"1000",
	"Test_while_idle":"true",
	"max_connections":"10",
	"preparedStatements":"false",
	"Session_close-up SQL":"",
	"schema":"public"
}
DIRECTORY_DEFAULT_PARAMETERS={
	"cache_and_reuse_memory_maps":"True",
	"namespace":"",
	"filetype":"shapefile",
	"charset":"GBK",
	"create_spatial_index":"True",
	"fstype":"shape",
	"url":"",
	"enable_spatial_index":"True",
	"memory_mapped_buffer":"False",
	"timezone":"Pacific Standard Time"
}
def get_paras_list(storetype=Datastoretype.shapefile):
	if storetype==Datastoretype.shapefile:
		return SHAPFILE_DEFAULT_PARAMETERS
	elif storetype==Datastoretype.postgis:
		return POSTGIS_DEFAULT_PARAMETERS
	elif storetype==Datastoretype.directory:
		return DIRECTORY_DEFAULT_PARAMETERS
	else:
		return SHAPFILE_DEFAULT_PARAMETERS
def generate_store_body(storetype,storename,**params):

	xml=ET.parse(DATASTORE_TEMPLATE)
	try:
		body=xml.findall("%s/dataStore" %storetype)[0]
	except Exception:
		logging.error('storetype should be one of Shapefile,PostGIS,Dirctory and Geopackage.default shapefile')

	body.findall("name")[0].text=storename

	for param in params.keys():
		print(params[param])
		if param=='url' and re.match('^file:/.*',param) is None:
			params[param]="file:"+os.path.abspath(params[param])
		body.findall(".//entry[@key='%s']" % param)[0].text=params[param]
	return ET.tostring(body).strip()
class DatastoreInfo(object):
	def __init__(self,storename,datastoretype=Datastoretype.shapefile,**kwargs):
		DEFAULT_PARAMETERS={"Shapefile":SHAPFILE_DEFAULT_PARAMETERS,
							"PostGIS":POSTGIS_DEFAULT_PARAMETERS,
							"Directory of spatial files (shapefiles)":\
							DIRECTORY_DEFAULT_PARAMETERS
							}
		_body=ET.Element('dataStore')
		_body.append(create_xml_node('name',text=storename))
		_body.append(create_xml_node('type',text=datastoretype.value))
		_body.append(create_xml_node('description',text='Shapefile created from REST'))
		_body.append(create_xml_node('connectionParameters'))

		_parameters=DEFAULT_PARAMETERS[datastoretype.value]
		for arg in kwargs.keys():
			if arg in _parameters.keys():
				if arg=='url' and re.match('^file:/.*',kwargs[arg]) is None:
					_url="file://"+os.path.abspath(kwargs[arg])
					_parameters[arg]=_url
				else:
					_parameters[arg]=kwargs[arg]
			else:
				raise Exception('invalid parameter "%s".use function \
"get_paras_list(datastoretype)" to view the list of parameters' % arg)
		for para in _parameters.keys():
			_body.find('connectionParameters').append(\
				entry_key(para.replace('_',' '),str(_parameters[para])))
		self.body=ET.tostring(_body)
		# print(self.body)
	def xml_body(self):
		return self.body


class Datastores(base_handler):
	def __init__(self,engine,workspace):
		self.engine=engine
		self.workspace=workspace


	@property
	def url(self):
		return build_url(self.engine.service_url,['workspaces',self.workspace.name,'datastores'])

	# @property
	# def body(self):
	# 	rst=self.http_request(self.url).json()

	# 	return rst#['dataStores']['dataStore']
	# @property
	# def items(self):
	# 	ds=self.body
	# 	return [d['name'] for d in ds]
	
	def __getitem__(self,store):
		if isinstance(store,int):
			return Datastore(self.engine,self.workspace,self.items[store])
		elif store in self.items:
			return Datastore(self.engine,self.workspace,store)
		else:
			raise Exception("datastore '%s' does not exist" % store)
	# def __getattr__(self,storename):
	# 	return self.__getitem__(storename)
	def drop(self,obj,recurse=False):
		if isinstance(obj,str):
			self[obj].drop(recurse)
		elif isinstance(obj,Datastore):
			obj.drop(recurse)
		else:
			raise Exception()

	def add(self,storename=None,storetype=Datastoretype.shapefile,\
		datastoreinfo=None,datastorebody=None,**kwargs):
		if datastorebody:
			storebody=datastorebody
		elif datastoreinfo:
			storebody=datastoreinfo.xml_body()
		else:
			if storename is None:
				raise Exception('Oee of the parameters "storename","datastoreinfo","datastorebody" must be specified.')
			storebody=DatastoreInfo(storename,storetype,**kwargs).xml_body()
		# print(storebody)
		self.add_from_xml(storebody)
	def add_from_xml(self,xmlbody):
		headers={'Accept':'application/json','content-type':'application/xml'}
		self.http_request(self.url,method='post',data=xmlbody,headers=headers)
		print('successful add a datastore.')
	

	def upload(self,storename,method,format,data,config=None,charset="GBK",filename=None,target=None,update=None):
		url=build_url(self.engine.service_url,['workspaces',self.workspace.name,'datastores',storename,'%s.%s' %(method,format)])
		headers = {'Content-type': 'application/zip'}
		# print(url)
		rst=self.http_request(url+"?filename=%s" % filename,method='put',data=data,headers=headers)
		print('upload successfully')
	


class Datastore(base_handler):
	def __init__(self,engine,workspace,name=None):
		self.engine=engine
		self.workspace=workspace
		self.name=name
		self._enabled='true'


	@property
	def url(self):
		return build_url(self.engine.service_url,['workspaces',self.workspace.name,'datastores',self.name])

	# @property
	# def body(self):
	# 	rst=self.get_body()
	# 	return rst['dataStore']
	@property
	def connectionParameters(self):
		_body=ET.fromstring(self.body).findall('.//connectionParameters/*')
		params={}
		for value in _body:
			for key in value.attrib.keys():
				params[value.attrib[key]]=value.text
		return params

	@property
	def _featuretypes_url(self):
		return ET.fromstring(self.body).findall('.//featureTypes/*')[0].attrib['href']
	@property
	def type(self):
		return ET.fromstring(self.body).findall('.//type')[0].text
	@property
	def layers(self):
		# return Layer(self.engine,self)
		lsbody=self.http_request(self._featuretypes_url).text
		lsname=ET.fromstring(lsbody).findall('.//name')
		
		# featurntypes=rst['featureTypes']['featureType']
		# ls=[]
		ls=[Layer(self.engine,name.text) for name in lsname]

		return ls
	@property
	def items(self):
		pass
	@property
	def enabled(self):
		return ET.fromstring(self.body).findall('enabled')[0].text
	@enabled.setter
	def enabled(self,enabled):
		_body=ET.fromstring(self.body)
		_body.findall('enabled')[0].text=str(enabled)
		self.http_request(self.url,method='put',data=ET.tostring(_body))

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
	
	
	

	# def get_body(self,type='json'):
	# 	if type =='json':
	# 		return self.http_request(self.url).json()
		# else :
		# 	headers={'Accept':'application/xml','content-type':'application/xml'}
		# 	return self.http_request(self.url,headers=headers).text
	def drop(self,recurse=False):
		delete_url=self.url
		if recurse:
			delete_url=self.url+"?recurse=true"
		rst=self.http_request(delete_url,method='delete')

		print('Successfully drop datastore "%s".' % self.name)

	
	def update(self,xmlbody=None,**parmas):
		headers={'Accept':'application/json','content-type':'application/xml'}
		if xmlbody:
			self.http_request(self.url,method='put',data=xmlbody,headers=headers)
		else:
			# storebody=generate_store_body(self.type,self.name,**parmas)
			storebody=self.body
			storebody=ET.fromstring(storebody)
			for parma in parmas:
				# item=storebody.findall('.//%s' % parma)
				# if len(item)==0:
				# 	item=storebody.findall('.//entry[@key="%s"]' % parma)
				# item[0]=parmas[parma]
				try:
					storebody.findall('.//%s' % parma)[0].text=parmas[parma]
				except :
					if parma=='url' and re.match('^file:/.*',parma) is None:
						parmas[parma]="file:"+os.path.abspath(parmas[parma])
					storebody.findall('.//entry[@key="%s"]' % parma)[0].text=parmas[parma]
			# print(ET.tostring(storebody))			
			self.http_request(self.url,method='put',data=ET.tostring(storebody),\
				headers=headers)



class CoveragestoreInfo(object):
	def __init__(self,name,coverstoretype=Coveragestoretype.geotiff,**kwargs):
		pass


class Coveragestores(base_handler):
	
	def __init__(self,engine=None,workspace=None):
		parameters_check(locals())
		self.engine=engine
		self.workspace=workspace
	@property
	def url(self):
		_url=build_url(self.engine.service_url,
			['workspaces',
			self.workspace.name,
			'coveragestores'
			])
		return _url
	def add(self,name=None,coveragestoretype=Coveragestoretype.geotiff,\
		coveragestorebody=None,**kwargs):
		if coveragestorebody:
			_coveragestorebody=coveragestorebody
			name=ET.fromstring(_coveragestorebody).findall('coverageStore/name')[0].text
		else:
			_coveragestorebody=self.generage_coverage_body(name,coveragestoretype,**kwargs)

		parameters_check(name=name)
		if name in self.items:
			raise Exception('coveragestore "%s" alreadly exists.' % name)
		self.http_request(self.url,method='post',data=_coveragestorebody)
		print('successful addition of coveragestore "%s"' % name)


	def generage_coverage_body(self,name,coveragestoretype=\
		Coveragestoretype.geotiff.value,**kwargs):
		_body=ET.Element('coverageStore')
		_body.append(create_xml_node('name',text=name))
		_disc=kwargs['discription'] if 'discription' in kwargs.keys() else name
		_body.append(create_xml_node('discription',text=_disc))
		_body.append(create_xml_node('type',text=coveragestoretype.value))
		_enabled=kwargs['enabled'] if 'enabled' in kwargs else 'true'
		_body.append(create_xml_node('enabled',text=_enabled))
		_body.append(create_xml_node('workspace'))
		_body.find('workspace').append(create_xml_node('name',text=self.workspace.name))
		ET.register_namespace('atom','http://www.w3.org/2005/Atom')
		_wslink=ET.Element('{http://www.w3.org/2005/Atom}link',{'href':\
				self.workspace.url+'.xml','rel':"alternate",'type':"application/xml"})
		_body.find('workspace').append(_wslink)
		_body.append(create_xml_node('__default',text='false'))
		_url=kwargs['url'] if 'url' in kwargs else '*.tif'
		_body.append(create_xml_node('url',text=_url))
		_body.append(create_xml_node('coverages'))
		_cslink=ET.Element('{http://www.w3.org/2005/Atom}link',{'href':\
				self.url+'/%s/coverages.xml' % name,'rel':"alternate",'type':"application/xml"})
		_body.find('coverages').append(_cslink)

		# print(ET.tostring(_body))
		return ET.tostring(_body)
	def upload(self,coveragestorename,method='file',format='GeoTIFF',config=None,\
		USE_JAI_IMAGEAD=None,coveragename=None,filename=None):
		_url=self.url+"/%s/%s.%s" % (coveragestorename,method,format)
		_paras="?"
		if config:
			_paras+="config=%s&" % config
		if USE_JAI_IMAGEAD:
			_paras+='USE_JAI_IMAGEAD=%s&' % USE_JAI_IMAGEAD
		if coveragename:
			_paras+='coveragename=%s&' % coveragename
		if filename:
			_paras+='filename=%s&' % filename
		_url=_url+_paras
		if method=='file':
			if filename is None:
				raise Exception("filename no assigned.")
			_data=open(filename,'rb').read()
			print(_url)

		self.http_request(_url,method='put',data=_data,headers={'content-type':'application/zip'})
		print('upload successfully.')
		
	def __getitem__(self,key):
		if isinstance(key,int):
			return Coveragestore(self.engine,self.workspace,self.items[key])
		if isinstance(key,str):
			if key in self.items:
				return Coveragestore(self.engine,self.workspace,key)
			else:
				raise Exception('coveragestores has not store "%s,%s".' % \
					(self.workspace.name,key))


class Coveragestore(base_handler):
	
	def __init__(self,engine=None,workspace=None,name=None):
		parameters_check(locals())
		self.engine=engine
		self.workspace=workspace
		self.name=name
	@property
	def url(self):
		_url=build_url(self.engine.service_url,
			['workspaces',
			self.workspace.name,
			'coveragestores',
			self.name
			])
		return _url
	@property
	def type(self):
		return ET.fromstring(self.body).find('type').text
	
	@property
	def items(self):
		pass

	def drop(self,recurse=False):
		_url="%s?recurse=%s" % (self.url,recurse)
		self.http_request(_url,method='delete')
		print('successfully droped coveragestore "%s"' % self.name)
	@property
	def enabled(self):
		return ET.fromstring(self.body).find('enabled').text
	@enabled.setter
	def enabled(self,value):
		_body=ET.fromstring(self.body)
		if value:
			_value='true'
		else:
			_value='false'
		_body.find('enabled').text=_value
		self.http_request(self.url,data=ET.tostring(_body),method='put')
	

	
	
	
	
class WMSstores(base_handler):
	def __init__(self,engine=None,workspace=None):
		parameters_check(locals())
		self.engine=engine
		self.workspace=workspace

	@property
	def url(self):
		_url=build_url(self.engine.service_url,['workspaces',self.workspace.name,'wmsstores'])
		return _url
	def __getitem__(self,key):
		if isinstance(key.int):
			return WMSstore(self.engine,self.workspace,self.items[key])
		elif isinstance(key,str):
			return WMSstore(self.engine,self.workspace,key)
		else:
			raise Exception("KeyError")


	
class WMSstore(base_handler):
	def __init__(self,engine=None,workspace=None,name=None):
		parameters_check(locals())
		self.engine=engine
		self.workspace=workspace
		self.name=name

	@property
	def url(self):
		_url=build_url(self.engine.service_url,
			['workspaces',
			self.workspace.name,
			'wmsstores',
			self.name
			])
		return _url
	@property
	def items(self):
		pass

	def drop(self,recurse=False):
		_url=self.url+"?recurse=%s" % recurse
		self.http_request(_url,method='delete')
		print('successfully drop wmsstore "%s" ' % self.name)

class WMTSstores(base_handler):
	def __init__(self,engine=None,workspace=None):
		parameters_check(locals())
		self.engine=engine
		self.workspace=workspace

	@property
	def url(self):
		_url=build_url(self.engine.service_url,['workspaces',self.workspace.name,'wmtsstores'])
		return _url
	def __getitem__(self,key):
		if isinstance(key.int):
			return WMTSstore(self.engine,self.workspace,self.items[key])
		elif isinstance(key,str):
			return WMSTstore(self.engine,self.workspace,key)
		else:
			raise Exception("KeyError")


class WMSstore(base_handler):
	def __init__(self,engine=None,workspace=None,name=None):
		parameters_check(locals())
		self.engine=engine
		self.workspace=workspace
		self.name=name

	@property
	def url(self):
		_url=build_url(self.engine.service_url,
			['workspaces',
			self.workspace.name,
			'wmtsstores',
			self.name
			])
		return _url
	@property
	def items(self):
		pass

	def drop(self,recurse=False):
		_url=self.url+"?recurse=%s" % recurse
		self.http_request(_url,method='delete')
		print('successfully drop wmtsstore "%s" ' % self.name)
	
	


	



# if __name__ == '__main__':
# 	engine=default_engine()
	# # ds=Datastore(engine,'test','China')
	# # ds.drop(True)
	# dss=Datastores(engine,'test')
	# # print(ds.url)
	# # print(dss['China'])
	# # print(dss.China)
	# # dss.add(Datastoretype.postgis.value,'China',host='localhost',user='postgres',passwd='postgres',database='test1',port='5432')
	# # dss.add('Shapefile','reststore',url='/home/zhang/Desktop/China_province.shp')
	# # print(dss.items)
	# # ds.update(url='/home/zhang/Desktop/weather_station2.shp')
	# # print(ds.type)
	# # print(ds.featuretypes)
	# ws=Workspace(engine,'test')
	# d=Datastore(engine,ws,'China')
	# print(d.body)
	# print(d.items)
	# print(d.connectionParameters)
	# print(d.type)
	# print(d.layers)
	# print(d.connectionParameters)
	# print(dss.items)
	# dss.China.drop(True)
	# dss.add(Datastoretype.postgis.value,'China',host='localhost',user='postgres',passwd='postgres',database='test1',port='5432')
	
	# dss.China.drop(True)
	# dss.add('Shapefile','wstest')
	# data=open("/home/zhang/Desktop/province.zip",'rb').read()
	# dss.upload('wstest','file','shp',data,filename='province999')
	# ds.drop(True)
	# dss.fafaafa.drop(True)
	# dss.wstest3.drop(True)
# 
	# ds.drop()
	# ds.drop()