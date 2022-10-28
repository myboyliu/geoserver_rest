# -*- coding: utf-8 -*-
# @Author: zhangbo
# @E-mail: xtfge_0915@163.com
# @Date:   2018-12-22 19:32:06
# @Last Modified by:   zhangbo
# @Last Modified time: 2019-01-08 21:01:16

import requests

def default_engine():
	return Engine('http://localhost:8080/geoserver/rest','admin','geoserver')
class Engine(object):

	def __init__(self,url,username,password):
		self.service_url=url
		self.user=username
		self.password=password




