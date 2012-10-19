# Copyright 2011-2012 OpenStack LLC.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import copy

from lxml import etree
from webob import exc
import webob

from nova.api.openstack.compute.contrib import cells as cells_ext
from nova.api.openstack import xmlutil
from nova.cells import rpcapi as cells_rpcapi
from nova import compute
from nova import context
from nova import db
from nova import exception
from nova import flags
from nova.openstack.common import jsonutils
from nova.openstack.common import timeutils
from nova import test
from nova.tests.api.openstack import fakes


FLAGS = flags.FLAGS


UUID1 = '00000000-0000-0000-0000-000000000001'
UUID2 = '00000000-0000-0000-0000-000000000002'
UUID3 = '00000000-0000-0000-0000-000000000003'


FAKE_CELLS = [
        dict(id=1, name='cell1', username='bob', is_parent=True,
                weight_scale=1.0, weight_offset=0.0,
                rpc_host='r1.example.org', password='xxxx'),
        dict(id=2, name='cell2', username='alice', is_parent=False,
                weight_scale=1.0, weight_offset=0.0,
                rpc_host='r2.example.org', password='qwerty')]


FAKE_CAPABILITIES = [
        {'cap1': '0,1', 'cap2': '2,3'},
        {'cap3': '4,5', 'cap4': '5,6'}]


def fake_db_cell_get(context, cell_id):
    try:
        return FAKE_CELLS[cell_id - 1]
    except IndexError:
        raise exception.CellNotFound


def fake_db_cell_create(context, values):
    cell = dict(id=1)
    cell.update(values)
    return cell


def fake_db_cell_update(context, cell_id, values):
    cell = fake_db_cell_get(context, cell_id)
    cell.update(values)
    return cell


def fake_cells_api_get_all_cell_info(*args):
    cells = copy.deepcopy(FAKE_CELLS)
    del cells[0]['password']
    del cells[1]['password']
    for i, cell in enumerate(cells):
        cell['capabilities'] = FAKE_CAPABILITIES[i]
    return cells


def fake_db_cell_get_all(context):
    return FAKE_CELLS


class CellsTest(test.TestCase):
    def setUp(self):
        super(CellsTest, self).setUp()
        self.stubs.Set(db, 'cell_get', fake_db_cell_get)
        self.stubs.Set(db, 'cell_get_all', fake_db_cell_get_all)
        self.stubs.Set(db, 'cell_update', fake_db_cell_update)
        self.stubs.Set(db, 'cell_create', fake_db_cell_create)
        self.stubs.Set(cells_rpcapi.CellsAPI, 'get_all_cell_info',
                fake_cells_api_get_all_cell_info)

        self.controller = cells_ext.Controller()
        self.context = context.get_admin_context()

    def _get_request(self, resource):
        return fakes.HTTPRequest.blank('/v2/fake/' + resource)

    def test_index(self):
        req = self._get_request("cells")
        res_dict = self.controller.index(req)

        self.assertEqual(len(res_dict['cells']), 2)
        for i, cell in enumerate(res_dict['cells']):
            self.assertEqual(cell['name'], FAKE_CELLS[i]['name'])
            self.assertNotIn('capabilitiles', cell)
            self.assertNotIn('password', cell)

    def test_detail(self):
        req = self._get_request("cells/detail")
        res_dict = self.controller.detail(req)

        self.assertEqual(len(res_dict['cells']), 2)
        for i, cell in enumerate(res_dict['cells']):
            self.assertEqual(cell['name'], FAKE_CELLS[i]['name'])
            self.assertEqual(cell['capabilities'], FAKE_CAPABILITIES[i])
            self.assertNotIn('password', cell)

    def test_show_nan_raises(self):
        req = self._get_request("cells/1")
        self.assertRaises(exc.HTTPBadRequest, self.controller.show, req, 'aoe')

    def test_show_no_cell_raises(self):
        req = self._get_request("cells/1")
        self.assertRaises(exc.HTTPNotFound, self.controller.show, req, '999')

    def test_get_cell_by_id(self):
        req = self._get_request("cells/1")
        res_dict = self.controller.show(req, 1)
        cell = res_dict['cell']

        self.assertEqual(cell['id'], 1)
        self.assertEqual(cell['rpc_host'], 'r1.example.org')
        self.assertNotIn('password', cell)

    def test_cell_delete(self):
        call_info = {'delete_called': 0}

        def fake_db_cell_delete(context, cell_id):
            self.assertEqual(cell_id, 999)
            call_info['delete_called'] += 1

        self.stubs.Set(db, 'cell_delete', fake_db_cell_delete)

        req = self._get_request("cells/999")
        self.controller.delete(req, 999)
        self.assertEqual(call_info['delete_called'], 1)

    def test_delete_nan_raises(self):
        req = self._get_request("cells/1")
        self.assertRaises(exc.HTTPBadRequest, self.controller.delete,
                req, 'aoe')

    def test_delete_nan_raises(self):
        req = self._get_request("cells/1")
        req.environ['nova.context'] = self.context
        self.assertRaises(exc.HTTPNotFound, self.controller.delete, req, '999')

    def test_cell_create_parent(self):
        body = {'cell': {'name': 'meow',
                        'username': 'fred',
                        'password': 'fubar',
                        'rpc_host': 'r3.example.org',
                        'type': 'parent',
                        # Also test this is ignored/stripped
                        'is_parent': False}}

        req = self._get_request("cells")
        res_dict = self.controller.create(req, body)
        cell = res_dict['cell']

        self.assertEqual(cell['id'], 1)
        self.assertEqual(cell['name'], 'meow')
        self.assertEqual(cell['username'], 'fred')
        self.assertEqual(cell['rpc_host'], 'r3.example.org')
        self.assertEqual(cell['type'], 'parent')
        self.assertNotIn('password', cell)
        self.assertNotIn('is_parent', cell)

    def test_cell_create_child(self):
        body = {'cell': {'name': 'meow',
                        'username': 'fred',
                        'password': 'fubar',
                        'rpc_host': 'r3.example.org',
                        'type': 'child'}}

        req = self._get_request("cells")
        res_dict = self.controller.create(req, body)
        cell = res_dict['cell']

        self.assertEqual(cell['id'], 1)
        self.assertEqual(cell['name'], 'meow')
        self.assertEqual(cell['username'], 'fred')
        self.assertEqual(cell['rpc_host'], 'r3.example.org')
        self.assertEqual(cell['type'], 'child')
        self.assertNotIn('password', cell)
        self.assertNotIn('is_parent', cell)

    def test_cell_create_no_name_raises(self):
        body = {'cell': {'username': 'moocow',
                         'password': 'secret',
                         'rpc_host': 'r3.example.org',
                         'type': 'parent'}}

        req = self._get_request("cells")
        self.assertRaises(exc.HTTPBadRequest,
            self.controller.create, req, body)

    def test_cell_create_name_empty_string_raises(self):
        body = {'cell': {'name': '',
                         'username': 'fred',
                         'password': 'secret',
                         'rpc_host': 'r3.example.org',
                         'type': 'parent'}}

        req = self._get_request("cells")
        self.assertRaises(exc.HTTPBadRequest,
            self.controller.create, req, body)

    def test_cell_create_name_with_bang_raises(self):
        body = {'cell': {'name': 'moo!cow',
                         'username': 'fred',
                         'password': 'secret',
                         'rpc_host': 'r3.example.org',
                         'type': 'parent'}}

        req = self._get_request("cells")
        self.assertRaises(exc.HTTPBadRequest,
            self.controller.create, req, body)

    def test_cell_create_name_with_dot_raises(self):
        body = {'cell': {'name': 'moo.cow',
                         'username': 'fred',
                         'password': 'secret',
                         'rpc_host': 'r3.example.org',
                         'type': 'parent'}}

        req = self._get_request("cells")
        self.assertRaises(exc.HTTPBadRequest,
            self.controller.create, req, body)

    def test_cell_create_name_with_invalid_type_raises(self):
        body = {'cell': {'name': 'moocow',
                         'username': 'fred',
                         'password': 'secret',
                         'rpc_host': 'r3.example.org',
                         'type': 'invalid'}}

        req = self._get_request("cells")
        self.assertRaises(exc.HTTPBadRequest,
            self.controller.create, req, body)

    def test_cell_update(self):
        body = {'cell': {'username': 'zeb',
                         'password': 'sneaky'}}

        req = self._get_request("cells/1")
        res_dict = self.controller.update(req, 1, body)
        cell = res_dict['cell']

        self.assertEqual(cell['id'], 1)
        self.assertEqual(cell['rpc_host'], FAKE_CELLS[0]['rpc_host'])
        self.assertEqual(cell['username'], 'zeb')
        self.assertNotIn('password', cell)

    def test_cell_update_empty_name_raises(self):
        body = {'cell': {'name': '',
                         'username': 'zeb',
                         'password': 'sneaky'}}

        req = self._get_request("cells/1")
        self.assertRaises(exc.HTTPBadRequest,
            self.controller.update, req, 1, body)

    def test_cell_update_invalid_type_raises(self):
        body = {'cell': {'username': 'zeb',
                         'type': 'invalid',
                         'password': 'sneaky'}}

        req = self._get_request("cells/1")
        self.assertRaises(exc.HTTPBadRequest,
            self.controller.update, req, 1, body)

    def test_cell_info(self):
        caps = ['cap1=a;b', 'cap2=c;d']
        self.flags(name='darksecret', capabilities=caps, group='cells')

        req = self._get_request("cells/info")
        res_dict = self.controller.info(req)
        cell = res_dict['cell']
        cell_caps = cell['capabilities']

        self.assertEqual(cell['name'], 'darksecret')
        self.assertEqual(cell_caps['cap1'], 'a;b')
        self.assertEqual(cell_caps['cap2'], 'c;d')

    def test_sync_instances(self):
        call_info = {}

        def sync_instances(self, context, **kwargs):
            call_info['project_id'] = kwargs.get('project_id')
            call_info['updated_since'] = kwargs.get('updated_since')

        self.stubs.Set(cells_rpcapi.CellsAPI, 'sync_instances', sync_instances)

        req = self._get_request("cells/sync_instances")
        body = {}
        self.controller.sync_instances(req, body=body)
        self.assertEqual(call_info['project_id'], None)
        self.assertEqual(call_info['updated_since'], None)

        body = {'project_id': 'test-project'}
        self.controller.sync_instances(req, body=body)
        self.assertEqual(call_info['project_id'], 'test-project')
        self.assertEqual(call_info['updated_since'], None)

        expected = timeutils.utcnow().isoformat()
        if not expected.endswith("+00:00"):
            expected += "+00:00"

        body = {'updated_since': expected}
        self.controller.sync_instances(req, body=body)
        self.assertEqual(call_info['project_id'], None)
        self.assertEqual(call_info['updated_since'], expected)

        body = {'updated_since': 'skjdfkjsdkf'}
        self.assertRaises(exc.HTTPBadRequest,
                self.controller.sync_instances, req, body=body)

        body = {'foo': 'meow'}
        self.assertRaises(exc.HTTPBadRequest,
                self.controller.sync_instances, req, body=body)


class TestCellsXMLSerializer(test.TestCase):
    def test_multiple_cells(self):
        fixture = {'cells': fake_cells_api_get_all_cell_info()}

        serializer = cells_ext.CellsTemplate()
        output = serializer.serialize(fixture)
        res_tree = etree.XML(output)

        self.assertEqual(res_tree.tag, '{%s}cells' % xmlutil.XMLNS_V10)
        self.assertEqual(len(res_tree), 2)
        self.assertEqual(res_tree[0].tag, '{%s}cell' % xmlutil.XMLNS_V10)
        self.assertEqual(res_tree[1].tag, '{%s}cell' % xmlutil.XMLNS_V10)

    def test_single_cell_with_caps(self):
        cell = {'id': 1,
                'name': 'darksecret',
                'capabilities': {'cap1': 'a;b',
                                 'cap2': 'c;d'}}
        fixture = {'cell': cell}

        serializer = cells_ext.CellTemplate()
        output = serializer.serialize(fixture)
        res_tree = etree.XML(output)

        self.assertEqual(res_tree.tag, '{%s}cell' % xmlutil.XMLNS_V10)
        self.assertEqual(res_tree.get('id'), '1')
        self.assertEqual(res_tree.get('name'), 'darksecret')
        self.assertEqual(res_tree.get('password'), None)
        self.assertEqual(len(res_tree), 1)

        child = res_tree[0]
        self.assertEqual(child.tag,
                '{%s}capabilities' % xmlutil.XMLNS_V10)
        for elem in child:
            self.assertIn(elem.tag, ('{%s}cap1' % xmlutil.XMLNS_V10,
                                      '{%s}cap2' % xmlutil.XMLNS_V10))
            if elem.tag == '{%s}cap1' % xmlutil.XMLNS_V10:
                self.assertEqual(elem.text, 'a;b')
            elif elem.tag == '{%s}cap2' % xmlutil.XMLNS_V10:
                self.assertEqual(elem.text, 'c;d')

    def test_single_cell_without_caps(self):
        cell = {'id': 1,
                'name': 'darksecret'}
        fixture = {'cell': cell}

        serializer = cells_ext.CellTemplate()
        output = serializer.serialize(fixture)
        res_tree = etree.XML(output)

        self.assertEqual(res_tree.tag, '{%s}cell' % xmlutil.XMLNS_V10)
        self.assertEqual(res_tree.get('id'), '1')
        self.assertEqual(res_tree.get('name'), 'darksecret')
        self.assertEqual(res_tree.get('password'), None)
        self.assertEqual(len(res_tree), 0)


class TestCellsXMLDeserializer(test.TestCase):
    def test_cell_deserializer(self):
        caps_dict = {'cap1': 'a;b',
                             'cap2': 'c;d'}
        caps_xml = ("<capabilities><cap1>a;b</cap1>"
                "<cap2>c;d</cap2></capabilities>")
        expected = {'cell': {'name': 'testcell1',
                             'type': 'child',
                             'rpc_host': 'localhost',
                             'capabilities': caps_dict}}
        intext = ("<?xml version='1.0' encoding='UTF-8'?>\n"
                "<cell><name>testcell1</name><type>child</type>"
                        "<rpc_host>localhost</rpc_host>"
                        "%s</cell>") % caps_xml
        deserializer = cells_ext.CellDeserializer()
        result = deserializer.deserialize(intext)
        self.assertEqual(dict(body=expected), result)


def fake_compute_get(*args, **kwargs):
    return fakes.stub_instance(1, uuid=UUID3, cell_name="top!child3!gchild1")


def fake_compute_get_all(*args, **kwargs):
    return [
        fakes.stub_instance(1, uuid=UUID1, cell_name="top!child1!gchild1"),
        fakes.stub_instance(2, uuid=UUID2, cell_name="top!child2!gchild1"),
    ]


class CellsServerTest(test.TestCase):
    content_type = 'application/json'
    prefix = 'os-cells:'

    def setUp(self):
        super(ExtendedStatusTest, self).setUp()
        fakes.stub_out_nw_api(self.stubs)
        self.stubs.Set(compute.api.API, 'get', fake_compute_get)
        self.stubs.Set(compute.api.API, 'get_all', fake_compute_get_all)

    def _make_request(self, url):
        req = webob.Request.blank(url)
        req.headers['Accept'] = self.content_type
        res = req.get_response(fakes.wsgi_app())
        return res

    def _get_server(self, body):
        return jsonutils.loads(body).get('server')

    def _get_servers(self, body):
        return jsonutils.loads(body).get('servers')

    def assertServerStates(self, server, **kwargs):
        for k, v in kwargs:
            self.assertEqual(server.get('%s%s' % (self.prefix, k), v)

    def test_show(self):
        url = '/v2/fake/servers/%s' % UUID3
        res = self._make_request(url)

        self.assertEqual(res.status_int, 200)
        self.assertServerStates(self._get_server(res.body),
                                cell_name='top!child3!gchild1')

    def test_detail(self):
        url = '/v2/fake/servers/detail'
        res = self._make_request(url)

        self.assertEqual(res.status_int, 200)
        for i, server in enumerate(self._get_servers(res.body)):
            self.assertServerStates(server,
                                    cell_name='top!child%s!gchild1' % (i+1))

    def test_no_instance_passthrough_404(self):

        def fake_compute_get(*args, **kwargs):
            raise exception.InstanceNotFound()

        self.stubs.Set(compute.api.API, 'get', fake_compute_get)
        url = '/v2/fake/servers/70f6db34-de8d-4fbd-aafb-4065bdfa6115'
        res = self._make_request(url)

        self.assertEqual(res.status_int, 404)


class CellsServerXmlTest(CellsServerTest):
    content_type = 'application/xml'
    prefix = '{%s}' % cells_ext.Cells.namespace

    def _get_server(self, body):
        return etree.XML(body)

    def _get_servers(self, body):
        return etree.XML(body).getchildren()
