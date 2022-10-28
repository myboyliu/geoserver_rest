# -*- coding: utf-8 -*-
# @Author: zhangbo
# @E-mail: xtfge_0915@163.com
# @Date:   2018-12-24 15:44:50
# @Last Modified by:   zhangbo
# @Last Modified time: 2019-01-09 04:37:16
from rest.tools import base_handler
from rest.tools import build_url
from rest.engine import default_engine
import lxml.etree as ET
from rest.tools import parameters_check
from rest.style import Style
class Layer(base_handler):
	def __init__(self,engine,layername,workspace=None):
		self.name=layername
		self.engine=engine
		self.workspace=workspace
	
	@property
	def url(self):
		if self.workspace:
			return build_url(self.engine.service_url,['workspaces',self.workspace.name,'layers',self.name])
		return build_url(self.engine.service_url,['layers',self.name])
	@property
	def items(self):
		pass
	

	# @property
	# def body(self):
	# 	return self.http_request(self.url).json()
	@property
	def type(self):
		return ET.fromstring(self.body).findall('type')[0].text
	@property
	def style(self):
		_stylename=ET.fromstring(self.body).find('defaultStyle/name').text
		_style=_stylename.split(':')
		if len(_style)==2:
			return Style(self.engine,_style[0],_style[1])
		return Style(self.engine,_style[0])
	@style.setter
	def style(self,style):
		self.set_style(style)

	def set_style(self,style):
		_body=ET.fromstring(self.body)
		_style=ET.Element('defaultStyle')
		ET.register_namespace('atom','http://www.w3.org/2005/Atom')
		if style.workspace:
			name=ET.Element('name')
			name.text="%s:%s" % (style.workspace.name,style.name)
			ws=ET.Element('workspace')
			ws.text=style.workspace.name
			attr={'rel':"alternate",'type':"application/xml",'href':style.url}
			link=ET.Element('{http://www.w3.org/2005/Atom}line',attr)
			_style.append(name)
			_style.append(ws)
			_style.append(link)
		else:
			name=ET.Element('name')
			name.text=style.name
			attr={'rel':"alternate",'type':"application/xml",'href':style.url}
			link=ET.Element('{http://www.w3.org/2005/Atom}line',attr)
			_style.append(name)
			_style.append(link)
		_body.remove(_body.find('defaultStyle'))
		_body.append(_style)
		self.http_request(self.url,data=ET.tostring(_body),method='put')
		print('successfully set style of layer "%s" to "%s"' % (self.name,style.name))

		# nodelist=_body.getchildren()
		# for node in nodelist:
		# 	if node.tags=='defaultStyle'


		

	@property
	def resource(self):
		pass

	def drop(self,recurse=True):
		self.http_request(self.url+'?recurse=%s' % recurse,method='delete')
		print('successfully drop layer %s' % self.name)

class Layers(base_handler):
	def __init__(self,engine,workspace=None):
		self.engine=engine
		self.workspace=workspace
	@property
	def url(self):
		if self.workspace:
			return build_url(self.engine.service_url,['workspaces',self.workspace.name,'layers'])
		return build_url(self.engine.service_url,'layers')
	@property
	def items(self):
		lysname=ET.fromstring(self.body).findall('.//name')
		return [ly.text for ly in lysname]
	def __getitem__(self,layer):
		if isinstance(layer,int):
			return Layer(self.engine,self.items[layer],self.workspace)
		elif layer in self.items:
			return Layer(self.engine,layer,self.workspace)
		else:
			raise Exception("Layer %s no exist." % layer)
	

class WMSLayers(base_handler):
	def __init__(self,engine=None,workspace=None,wmsstore=None):
		parameters_check(engine=engine,workspace=workspace)
		self.engine=engine
		self.workspace=workspace
		self.wmsstore=wmsstore

	@property
	def url(self):
		_args=['workspaces',self.workspace.name]
		if self.wmsstore:
			_args+=['wmsstores',self.wmsstore.name]
		_args.append('wmslayers')
		return build_url(self.engine.service_url,_args)

	def __getitem__(self,key):
		if isinstance(key,int):
			return WMSLayer(self.engine,self.workspace,self.wmsstore,self.items[key])
		elif isinstance(key,str):
			return WMSLayer(self.engine,self.workspace,self.wmsstore,key)
		else:
			raise Exception("KeyError.")

class WMSLayer(base_handler):
	def __init__(self,engine=None,workspace=None,wmsstore=None,name=None):
		parameters_check(engine=engine,workspace=workspace,name=name)
		self.engine=engine
		self.workspace=workspace
		self.wmsstore=wmsstore
		self.name=name
	@property
	def url(self):
		_args=['workspaces',self.workspace.name]
		if self.wmsstore:
			_args+=['wmsstores',self.wmsstore.name]
		_args.append('wmslayers')
		_args.append(self.name)
		return build_url(self.engine.service_url,_args)
	@property
	def items(self):
		pass

	def drop(self,recurse=False):
		_url=self.url+'?recurse=%s' % recurse
		self.http_request(_url,method='delete')
		print('successfully drop WMSLayer "%s".' % self.name)



class WMTSLayers(base_handler):
	def __init__(self,engine=None,workspace=None,wmtsstore=None):
		parameters_check(engine=engine,workspace=workspace)
		self.engine=engine
		self.workspace=workspace
		self.wmtsstore=wmtsstore

	@property
	def url(self):
		_args=['workspaces',self.workspace.name]
		if self.wmtsstore:
			_args+=['wmtsstores',self.wmsstore.name]
		_args.append('wmtslayers')
		return build_url(self.engine.service_url,_args)

	def __getitem__(self,key):
		if isinstance(key,int):
			return WMSLayer(self.engine,self.workspace,self.wmsstore,self.items[key])
		elif isinstance(key,str):
			return WMSLayer(self.engine,self.workspace,self.wmsstore,key)
		else:
			raise Exception("KeyError.")

class WMTSLayer(base_handler):
	def __init__(self,engine=None,workspace=None,wmtsstore=None,name=None):
		parameters_check(engine=engine,workspace=workspace,name=name)
		self.engine=engine
		self.workspace=workspace
		self.wmtsstore=wmtsstore
		self.name=name
	@property
	def url(self):
		_args=['workspaces',self.workspace.name]
		if self.wmtsstore:
			_args+=['wmtsstores',self.wmtsstore.name]
		_args.append('wmslayers')
		_args.append(self.name)
		return build_url(self.engine.service_url,_args)
	@property
	def items(self):
		pass

	def drop(self,recurse=False):
		_url=self.url+'?recurse=%s' % recurse
		self.http_request(_url,method='delete')
		print('successfully drop WMTSLayer "%s".' % self.name)
	

	
		

# if __name__ == '__main__':
# 	l=Layer(default_engine(),'China_province')
# 	ls=Layers(default_engine(),'test')

# 	# print(l.body)
# 	# print(l.type)
# 	print(ls.body)
# 	print(ls.items)
	# ls['china_province'].drop()
	# print(l.body)
	# print(l.items)
	# ls['test:yunnan'].drop()
	# l.drop(True)
	# l.drop()
	