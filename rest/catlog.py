# -*- coding: utf-8 -*-
# @Author: zhangbo
# @E-mail: xtfge_0915@163.com
# @Date:   2019-01-08 20:10:49
# @Last Modified by:   zhangbo
# @Last Modified time: 2019-01-09 04:47:40
from rest.workspace import Workspace, Workspaces
from rest.layer import Layers, Layer
from rest.style import Styles, Style, StyleType
from rest.store import Datastore, Datastores, Coveragestore, Coveragestores, get_paras_list
from rest.engine import default_engine, Engine
import sys
from rest.tools import Datastoretype, Coveragestoretype
from rest.featuretype import Featuretypes, Featuretype
from rest.coverage import Coverage, Coverages
from rest.style import Styles, Style


class Catlog(object):
    def __init__(self, service_url=None, user='admin', password='geoserver', workspace=None, datastore=None,
                 coveragestore=None):
        if service_url is None:
            service_url = 'http://localhost:8080/geoserver/rest'
        self.engine = Engine(service_url, user, password)
        self.workspaces = Workspaces(self.engine)
        if workspace:
            self._workspace = workspace
        if datastore:
            self._datastore = datastore
        if coveragestore:
            self._coveragestore = coveragestore

    @property
    def workspace(self):
        if hasattr(self, '_workspace'):
            return self._workspace
        raise AttributeError('workspace has not specificed.')

    @workspace.setter
    def workspace(self, value):
        if isinstance(value, str):
            self._workspace = Workspace(self.engine, value)
        elif isinstance(value, Workspace):
            self._workspace = value
        else:
            raise ValueError('workspace "%s" invalid' % value)

    @property
    def datastores(self):
        return Datastores(self.engine, self.workspace)

    @property
    def datastore(self):
        if hasattr(self, '_datastore'):
            return self._datastore
        raise AttributeError('datastore has not specificed.')

    @property
    def layers(self):
        return self.get_layers(self.workspace)

    def get_layers(self, workspace=None):
        if workspace is None:
            return Layers(self.engine)
        elif isinstance(workspace, Workspace):
            return Layers(self.engine, self.workspace)
        elif isinstance(workspace, str):
            return Layers(self.engine, Workspace(self.engine, workspace))

    def add_workspace(self, name, uri=None):
        self.workspaces.add(name, uri)

    def drop_workspace(self, workspace, recurse=False):
        if isinstance(workspace, Workspace):
            _ws = workspace
        elif isinstance(workspace, str):
            _ws = Workspace(self.engine, workspace)
        elif isinstance(workspace, list):
            for w in workspace:
                if isinstance(w, str):
                    Workspace(self.engine, w).drop(recurse)
                else:
                    w.drop(recurse)
        else:
            raise TypeError('workspace defined error.')
        if _ws == self.workspace or _ws.name == self.workspace.name:
            if recurse == False:
                print(
                    'warning:workspace "%s" is being used.specify "recurse" parameter to be True,if you are sure to delete it.')
                sys.exit()
        _ws.drop()

    @datastore.setter
    def datastore(self, value):
        if isinstance(value, str):
            self._datastore = Datastore(self.engine, self.workspace, value)
        elif isinstance(value, Datastore):
            self._datastore = value
        else:
            raise ValueError('datastore "%s" is invalid.' % value)

    def add_datastore(self, storename=None, storetype=Datastoretype.shapefile, datastoreinfo=None, datastorebody=None,
                      **kwargs):
        self.datastores.add(storename, storetype, datastoreinfo, datastorebody, **kwargs)

    def drop_datastore(self, ds, recurse=False):
        def drop(ds):
            if isinstance(ds, str):
                self.datastores[ds].drop(recurse)
            elif isinstance(ds, Datastore):
                ds.drop(recurse)
            else:
                raise Exception('datastore "%s" no exists.' % ds)

        if isinstance(ds, list):
            for d in ds:
                drop(d)
        else:
            drop(ds)

    @property
    def coveragestores(self):
        return Coveragestores(self.engine, self.workspace)

    @property
    def coveragestore(self):
        if hasattr(self, "_coveragestore"):
            return self._coveragestore
        raise AttributeError('coveragestore has not specificed.')

    @coveragestore.setter
    def coveragestore(self, value):
        if isinstance(value, str):
            self._coveragestore = Coveragestore(self.engine, self.workspace, value)
        elif isinstance(value, Coveragestore):
            self._coveragestore = value
        else:
            raise ValueError('coveragestore error.')

    def add_coveragestore(self, name=None, coveragestoretype=Coveragestoretype.geotiff, coveragestorebody=None,
                          **kwargs):
        self.coveragestores.add(name, coveragestoretype, coveragestorebody, **kwargs)

    def drop_coveragestore(self, coveragestore, recurse=False):
        def drop(cs, recurse):
            if isinstance(cs, str):
                Coveragestore(self.engine, self.workspace, cs).drop(recurse)
            if isinstance(cs, Coveragestore):
                cs.drop(recurse)

        if isinstance(coveragestore, list):
            for cs in coveragestore:
                drop(cs, recurse)
        else:
            drop(coveragestore, recurse)

    @property
    def featuretypes(self):
        return self.get_featuretypes(self.workspace, self.datastore)

    @property
    def coverages(self):
        return self.get_coverages(self.workspace, self.coveragestore)

    def get_featuretypes(self, datastore=None):
        return Featuretypes(self.engine, self.workspace, datastore)

    def get_coverages(self, coveragestore=None):
        return Coverages(self.engine, self.workspace, coveragestore)

    def publish_featuretype(self, nativename, name=None, datastore=None, password=None, feature_setter_function=None):
        _datastore = datastore if datastore else self.datastore
        self.get_featuretypes(_datastore).add(nativename, name, datastore, password, feature_setter_function)

    def publist_coverage(self, nativename=None, coveragename=None, coveragestore=None,
                         coveragestoretype=Coveragestoretype.geotiff, setter_function=None):
        _coveragestore = coveragestore if coveragestore else self.coverages
        self.get_coverages(_coveragestore).add(nativename, coveragename, coveragestore, coveragestoretype,
                                               setter_function)

    def publish_layer(self, nativename=None, layername=None, store=None, storetype=Datastoretype.shapefile,
                      layer_setter_function=None, password=None):
        if isinstance(store, str):
            if storetype in Datastoretype:
                _store = Datastore(self.engine, self.workspace, store)
            if storetype in Coveragestoretype:
                _store = Coveragestore(self.engine, self.workspace, store)
        else:
            _store = store
        if _store.type in ['Shapefile', 'PostGIS', 'Directory of spatial files (shapefiles)', 'GeoPackage',
                           'PostGIS (JNDI)', 'Properties', 'Web Feature Server (NG)']:
            self.publish_featuretype(nativename, layername, _store, password, layer_setter_function)
        else:
            self.publist_coverage(nativename, layername, _store, storetype, layer_setter_function)

    def create_workspace(self, name, uri=None):
        if name in self.workspaces.items:
            return Workspace(self.engine, name)
        self.add_workspace(name, uri)
        return Workspace(self.engine, name)

    def add_coveragestore(self, name=None, coveragestoretype=Coveragestoretype.geotiff, coveragestorebody=None,
                          **kwargs):
        self.coveragestores.add(name, coveragestoretype, coveragestorebody, **kwargs)

    def add_store(self, storename=None, storetype=Datastoretype.shapefile, storeinfo=None, storebody=None, **kwargs):
        if storetype in Datastoretype:
            self.add_datastore(storename, storetype, storeinfo, storebody, **kwargs)
        if storetype in Coveragestoretype:
            self.add_coveragestore(storename, storetype, storebody, **kwargs)

    def drop_layer(self, layer, recurse=False):
        if isinstance(layer, str):
            self.layers[layer].drop(recurse)
        if isinstance(layer, Layer):
            layer.drop(recurse)

    def get_styles(self, workspace=None, include_workspace=False):
        if workspace is None:
            return Styles(self.engine).all_styles(include_workspace)

        return Styles(self.engine, self.workspace)

    @property
    def styles(self):
        return Styles(self.engine, self.workspace)

    def add_style(self, name=None, styletype=StyleType.sld, stylebody=None):
        self.styles.add(name, styletype, stylebody)

    def datasotre_parameters_list(storetype):
        return get_paras_list(storetype)
