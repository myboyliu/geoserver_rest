# -*- coding: utf-8 -*-
# @Author: zhangbo
# @E-mail: xtfge_0915@163.com
# @Date:   2018-12-25 04:05:15
# @Last Modified by:   zhangbo
# @Last Modified time: 2019-01-09 04:39:39

from rest.engine import default_engine
from rest.tools import build_url,base_handler
import lxml.etree as ET
import os
from rest.tools import parameters_check,StyleType

class Style(base_handler):
	def __init__(self,engine,workspace=None,name=None):
		parameters_check(name=name)
		self.engine=engine
		self.name=name
		self.workspace=workspace
	@property
	def url(self):
		_args=[]
		if self.workspace:
			_args+=['workspaces',self.workspace.name]
		_args+=['styles',self.name]
		return build_url(self.engine.service_url,_args)
	


class Styles(base_handler):
	def __init__(self,engine,workspace=None):

		self.engine=engine
		self.workspace=workspace

	@property
	def url(self):
		_args=[]
		if self.workspace:
			_args+=['workspaces',self.workspace.name]
		_args.append('styles')
		return build_url(self.engine.service_url,_args)


	def get_items_by_workspace(self,workspace,include_workspace=True):
		url=build_url(self.engine.service_url,['workspaces',workspace,'styles'])
		body=self.http_request(url,headers={'Accept':'application/xml'}).text
		items=ET.fromstring(body)
		if include_workspace:
			return ["%s@%s" %(item.text,workspace) for item in items.findall('.//name')]
		else:
			return[item.text for item in items.findall('.//name')]
	def all_styles(self,include_workspace=True):
		from workspace import Workspaces
		wss=Workspaces(self.engine)
		wsname=wss.items
		styles=self.items
		if include_workspace:
			styles=["%s@%s" % (style,"") for style in styles]
		for ws in wsname:
			if include_workspace:
				styles+=self.get_items_by_workspace(ws)
			else:
				styles+=self.get_items_by_workspace(ws,include_workspace)
		return styles
	def add(self,name=None,styletype=StyleType.sld,stylebody=None):
		# headers={'content-type':'application/json,application/zip,application/vnd.ogc.sld+xml,application/vnd.ogc.se+xml'}
		headers={'content-type':'%s;charste=utf-8' % styletype.value}
		if name:
			_url=self.url+"?name=%s" % name

		else:
			_url=self.url
		self.http_request(_url,method='post',data=stylebody,headers=headers)
		print('successfully add style "%s".' % name)
	def __getitem__(self,key):
		if isinstance(key,int):
			return Style(self.engine,self.workspace,self.items[key])
		elif isinstance(key,str):
			return Style(self.engine,self.workspace,key)
		else:
			raise Exception("KeyError.")

		
class Style(base_handler):
	def __init__(self,engine,workspace=None,name=None):
		parameters_check(name=name)
		self.engine=engine
		self.name=name
		self.workspace=workspace
	@property
	def url(self):
		if self.workspace:
			return build_url(self.engine.service_url,['workspaces',self.workspace.name,'styles',self.name])
		return build_url(self.engine.service_url,['styles',self.name])
	@property
	def format(self):
		return ET.fromstring(self.body).findall('format')[0].text
	@property
	def filename(self):
		return ET.fromstring(self.body).findall('filename')[0].text

	def drop(self,recurse=False):
		self.http_request(self.url+"?recurse=%s" % recurse,method='delete')
		print('sucessfully drop style "%s"' % self.name)
	
	
		


if __name__ == '__main__':
	ss=Styles(default_engine())
	print(ss.url)
	print(ss.items)
	# print(ss.get_items_by_workspace('test'))
	# print(ss.get_items())
	# s=open('/home/zhang/Desktop/qgis.sld').read()

	# # ss.add(s.encode('utf-8'),'test')
	# s=Style(default_engine(),"China_provice")
	# # print(s.body)
	# print(s.format)
	# print(s.filename)
	# s.drop(True)
	