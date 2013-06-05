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
"""
Tests For Cells Consistency Handler
"""
import random

from nova.cells import consistency
from nova import context
from nova import db
from nova import exception
from nova import test


class CellsConsistencyHandlerTestCase(test.TestCase):
    """Test case for CellsConsistencyManager class."""

    def setUp(self):
        super(CellsConsistencyHandlerTestCase, self).setUp()
        self.interval = 1
        self.update_number = 5
        self.flags(heal_update_interval=self.interval,
                   heal_update_number=self.update_number,
                   # heal_updated_at_threshold
                   group='cells')
        self.ctxt = context.RequestContext('fake', 'fake')
        self.now = 1369634796.45673
        self.db = db
        self.handler = consistency.ConsistencyHandler(self.db)
        self.fake_entries = [
            {'deleted': False, 'name': 'first'},
            {'deleted': False, 'name': 'second'},
            {'deleted': False, 'name': 'third'},
            {'deleted': False, 'name': 'fourth'},
            {'deleted': False, 'name': 'fifth'},
            {'deleted': False, 'name': 'sixth'},
        ]
        self.fake_entries_delete = [
            {'deleted': True, 'name': 'seventh'},
            {'deleted': True, 'name': 'eigth'},
        ]

    def test_is_time_to_heal(self):
        last_heal_time = self.now
        after_heal_time = last_heal_time + self.interval + 1
        before_heal_time = last_heal_time + self.interval - 1

        self.handler.set_last_heal_time(last_heal_time)

        tth = self.handler._is_time_to_heal(before_heal_time)
        self.assertFalse(tth)
        tth = self.handler._is_time_to_heal(after_heal_time)
        self.assertTrue(tth)

        self.handler.set_last_heal_time(last_heal_time + 5)
        tth = self.handler._is_time_to_heal(after_heal_time)
        self.assertFalse(tth)

    def test_heal_entries_not_time_to_heal(self):
        self.mox.StubOutWithMock(self.handler, '_current_time')
        self.mox.StubOutWithMock(self.handler, '_is_time_to_heal')
        # Not called.
        self.mox.StubOutWithMock(self.handler, '_heal_entries')

        self.handler._current_time().AndReturn(self.now)
        self.handler._is_time_to_heal(self.now).AndReturn(False)
        self.mox.ReplayAll()

        self.handler.heal_entries(self.ctxt)

    def test_heal_entries_time_to_heal(self):
        self.mox.StubOutWithMock(self.handler, '_current_time')
        self.mox.StubOutWithMock(self.handler, '_is_time_to_heal')
        self.mox.StubOutWithMock(self.handler, '_heal_entries')

        self.handler._current_time().AndReturn(self.now)
        self.handler._is_time_to_heal(self.now).AndReturn(True)
        self.handler._heal_entries(self.ctxt)
        self.mox.ReplayAll()

        self.handler.heal_entries(self.ctxt)

    def test__heal_entries(self):
        # Test that the consistency handler only sends 3 entries
        # when the db returns 3 entries, but the max number of
        # entries for the update is 5
        self.mox.StubOutWithMock(self.handler, 'get_entries_filtered')
        self.mox.StubOutWithMock(self.handler, '_send_create')
        self.mox.StubOutWithMock(self.handler, '_send_destroy')
        self.mox.stubs.Set(random, 'shuffle', lambda l: l)

        filters = {}
        entries = self.fake_entries[:3]
        self.handler.get_entries_filtered(self.ctxt, filters,
            'deleted', 'asc').AndReturn(entries)
        for entry in entries:
            self.handler._send_create(self.ctxt, entry)

        self.mox.ReplayAll()

        self.handler._heal_entries(self.ctxt)
        # Check that no more entries are sent after the first iteration
        # unless reset is called
        self.handler._heal_entries(self.ctxt)

    def test__heal_entries_with_reset(self):
        self.mox.StubOutWithMock(self.handler, 'get_entries_filtered')
        self.mox.StubOutWithMock(self.handler, '_send_create')
        self.mox.StubOutWithMock(self.handler, '_send_destroy')
        self.mox.stubs.Set(random, 'shuffle', lambda l: l)

        filters = {}
        entries = self.fake_entries

        # The first call should send the first 5 entries
        self.handler.get_entries_filtered(self.ctxt, filters,
            'deleted', 'asc').AndReturn(entries)
        for entry in entries[:5]:
            self.handler._send_create(self.ctxt, entry)

        # The second call after reset should send the remaining
        # entry from the previous list and 4 from the next call.
        self.handler.get_entries_filtered(self.ctxt, filters,
            'deleted', 'asc').AndReturn(entries)
        for entry in entries[5:] + entries[:4]:
            self.handler._send_create(self.ctxt, entry)

        self.mox.ReplayAll()

        self.handler._heal_entries(self.ctxt)
        self.handler.reset()
        self.handler._heal_entries(self.ctxt)


class CellsGroupConsistencyHandlerTestCase(test.TestCase):
    """Test case for GroupConsistencyHandler class."""
    def setUp(self):
        super(CellsGroupConsistencyHandlerTestCase, self).setUp()
        self.ctxt = context.RequestContext('fake', 'fake')
        self.db = db
        self.project_id = 'fake_project'
        self.name = 'fake_name'
        self.group = {'project_id': self.project_id, 'name': self.name}
        self.filters = {'project_id': self.project_id,
                        'name': self.name,
                        'deleted': False}
        self.handler = consistency.GroupConsistencyHandler(self.db)

    def test_send_destroy(self, should_destroy=True):
        handler = self.handler
        self.mox.StubOutWithMock(self.db,
                                 'security_group_get_all_by_filters')
        self.mox.StubOutWithMock(handler.cells_rpcapi,
                                 'security_group_destroy')

        # send_destroy should ignore the call if there are existing
        # undeleted groups.
        groups = [] if should_destroy else [self.group]
        self.db.security_group_get_all_by_filters(self.ctxt, self.filters,
            'created_at', 'desc').AndReturn(groups)
        if should_destroy:
            handler.cells_rpcapi.security_group_destroy(self.ctxt, self.group)
        self.mox.ReplayAll()

        handler._send_destroy(self.ctxt, self.group)

    def test_send_destroy_ignore_for_undeleted_group(self):
        self.test_send_destroy(should_destroy=False)


class CellsRulesConsistencyHandlerTestCase(test.TestCase):
    """Test case for RulesConsistencyHandler class."""
    def setUp(self):
        super(CellsRulesConsistencyHandlerTestCase, self).setUp()
        self.ctxt = context.RequestContext('fake', 'fake')
        self.db = db
        self.handler = consistency.RuleConsistencyHandler(self.db)

    def test_send_create(self):
        handler = self.handler
        group = {'fake': 'fake'}
        parent_group_id = 1
        rule = {'parent_group_id': parent_group_id}

        self.mox.StubOutWithMock(self.db, 'security_group_get')
        self.mox.StubOutWithMock(handler.cells_rpcapi,
                                 'security_group_rule_create')

        self.db.security_group_get(self.ctxt, parent_group_id).AndReturn(group)
        handler.cells_rpcapi.security_group_rule_create(self.ctxt, group, rule)
        self.mox.ReplayAll()

        handler._send_create(self.ctxt, rule)

    def test_send_create_no_group(self):
        handler = self.handler
        parent_group_id = 1
        rule = {'parent_group_id': parent_group_id}

        self.mox.StubOutWithMock(self.db, 'security_group_get')
        self.mox.StubOutWithMock(handler.cells_rpcapi,
                                 'security_group_rule_create')

        self.db.security_group_get(self.ctxt, parent_group_id).AndRaise(
            exception.SecurityGroupNotFound(security_group_id=parent_group_id))
        self.mox.ReplayAll()

        handler._send_create(self.ctxt, rule)

    def test_send_destroy(self, should_destroy=False):
        parent_group_id = 42
        rule = {'to_port': '80', 'from_port': '80',
                'parent_group_id': parent_group_id, 'protocol': 'tcp',
                'cidr': '0.0.0.0/0', 'group_id': '142'}
        group = {'fake': 'fake'}
        filters = rule.copy()
        filters['deleted'] = False
        handler = self.handler

        self.mox.StubOutWithMock(self.db,
                                 'security_group_rule_get_all_by_filters')
        self.mox.StubOutWithMock(self.db, 'security_group_get')
        self.mox.StubOutWithMock(handler.cells_rpcapi,
                                 'security_group_rule_create')

        rules = [] if should_destroy else [rule]
        self.db.security_group_rule_get_all_by_filters(self.ctxt,
            filters, 'created_at', 'desc').AndReturn(rules)
        if should_destroy:
            self.db.security_group_get(self.ctxt, parent_group_id).AndReturn(
                group)
            handler.cells_rpcapi.security_group_rule_destroy(self.ctxt,
                                                             group, rule)
        self.mox.ReplayAll()

        handler._send_destroy(self.ctxt, rule)

    def test_send_destroy_for_undeleted_rule(self):
        self.test_send_destroy(should_destroy=False)
