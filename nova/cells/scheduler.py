# Copyright (c) 2012 Rackspace Hosting
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

"""
Cells Scheduler
"""
import copy
import time

from oslo.config import cfg

from nova.cells import filters
from nova.cells import weights
from nova import compute
from nova.compute import instance_actions
from nova.compute import utils as compute_utils
from nova.compute import vm_states
from nova.db import base
from nova import exception
from nova.openstack.common import log as logging
from nova.scheduler import rpcapi as scheduler_rpcapi

cell_scheduler_opts = [
        cfg.ListOpt('scheduler_filter_classes',
                default=['nova.cells.filters.all_filters'],
                help='Filter classes the cells scheduler should use.  '
                        'An entry of "nova.cells.filters.all_filters"'
                        'maps to all cells filters included with nova.'),
        cfg.ListOpt('scheduler_weight_classes',
                default=['nova.cells.weights.all_weighers'],
                help='Weigher classes the cells scheduler should use.  '
                        'An entry of "nova.cells.weights.all_weighers"'
                        'maps to all cell weighers included with nova.'),
        cfg.IntOpt('scheduler_retries',
                default=10,
                help='How many retries when no cells are available.'),
        cfg.IntOpt('scheduler_retry_delay',
                default=2,
                help='How often to retry in seconds when no cells are '
                        'available.')
]

LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.register_opts(cell_scheduler_opts, group='cells')


class CellsScheduler(base.Base):
    """The cells scheduler."""

    def __init__(self, msg_runner):
        super(CellsScheduler, self).__init__()
        self.msg_runner = msg_runner
        self.state_manager = msg_runner.state_manager
        self.compute_api = compute.API()
        self.scheduler_rpcapi = scheduler_rpcapi.SchedulerAPI()
        self.filter_handler = filters.CellFilterHandler()
        self.filter_classes = self.filter_handler.get_matching_classes(
                CONF.cells.scheduler_filter_classes)
        self.weight_handler = weights.CellWeightHandler()
        self.weigher_classes = self.weight_handler.get_matching_classes(
                CONF.cells.scheduler_weight_classes)

    def _create_instances_here(self, ctxt, request_spec):
        instance_values = request_spec['instance_properties']
        num_instances = len(request_spec['instance_uuids'])
        for i, instance_uuid in enumerate(request_spec['instance_uuids']):
            instance_values['uuid'] = instance_uuid
            instance = self.compute_api.create_db_entry_for_new_instance(
                    ctxt,
                    request_spec['instance_type'],
                    request_spec['image'],
                    instance_values,
                    request_spec['security_group'],
                    request_spec['block_device_mapping'],
                    num_instances, i)

            self.msg_runner.instance_update_at_top(ctxt, instance)

    def _create_action_here(self, ctxt, instance_uuids):
        for instance_uuid in instance_uuids:
            action = compute_utils.pack_action_start(ctxt, instance_uuid,
                    instance_actions.CREATE)
            self.db.action_start(ctxt, action)

    def _get_possible_cells(self):
        cells = self.state_manager.get_child_cells()
        our_cell = self.state_manager.get_my_state()
        # Include our cell in the list, if we have any capacity info
        if not cells or our_cell.capacities:
            cells.append(our_cell)
        return cells

    def _run_instance(self, message, host_sched_kwargs):
        """Attempt to schedule instance(s).  If we have no cells
        to try, raise exception.NoCellsAvailable
        """
        ctxt = message.ctxt
        routing_path = message.routing_path
        request_spec = host_sched_kwargs['request_spec']

        LOG.debug(_("Scheduling with routing_path=%(routing_path)s"),
                  {'routing_path': routing_path})

        filter_properties = copy.copy(host_sched_kwargs['filter_properties'])
        filter_properties.update({'context': ctxt,
                                  'scheduler': self,
                                  'routing_path': routing_path,
                                  'host_sched_kwargs': host_sched_kwargs,
                                  'request_spec': request_spec})

        cells = self._get_possible_cells()
        cells = self.filter_handler.get_filtered_objects(self.filter_classes,
                                                         cells,
                                                         filter_properties)
        # NOTE(comstud): I know this reads weird, but the 'if's are nested
        # this way to optimize for the common case where 'cells' is a list
        # containing at least 1 entry.
        if not cells:
            if cells is None:
                # None means to bypass further scheduling as a filter
                # took care of everything.
                return
            raise exception.NoCellsAvailable()

        weighted_cells = self.weight_handler.get_weighed_objects(
                self.weigher_classes, cells, filter_properties)
        LOG.debug(_("Weighted cells: %(weighted_cells)s"),
                  {'weighted_cells': weighted_cells})

        # Keep trying until one works
        for weighted_cell in weighted_cells:
            cell = weighted_cell.obj
            try:
                if cell.is_me:
                    # Need to create instance DB entry as scheduler
                    # thinks it's already created... At least how things
                    # currently work.
                    self._create_instances_here(ctxt, request_spec)
                    # Need to record the create action in the db as the
                    # scheduler expects it to already exist.
                    self._create_action_here(
                            ctxt, request_spec['instance_uuids'])
                    self.scheduler_rpcapi.run_instance(ctxt,
                            **host_sched_kwargs)
                    return
                # Forward request to cell
                self.msg_runner.schedule_run_instance(ctxt, cell,
                                                      host_sched_kwargs)
                return
            except Exception:
                LOG.exception(_("Couldn't communicate with cell '%s'") %
                        cell.name)
        # FIXME(comstud): Would be nice to kick this back up so that
        # the parent cell could retry, if we had a parent.
        msg = _("Couldn't communicate with any cells")
        LOG.error(msg)
        raise exception.NoCellsAvailable()

    def run_instance(self, message, host_sched_kwargs):
        """Pick a cell where we should create a new instance."""
        try:
            for i in xrange(max(0, CONF.cells.scheduler_retries) + 1):
                try:
                    return self._run_instance(message, host_sched_kwargs)
                except exception.NoCellsAvailable:
                    if i == max(0, CONF.cells.scheduler_retries):
                        raise
                    sleep_time = max(1, CONF.cells.scheduler_retry_delay)
                    LOG.info(_("No cells available when scheduling.  Will "
                            "retry in %(sleep_time)s second(s)"), locals())
                    time.sleep(sleep_time)
                    continue
        except Exception:
            request_spec = host_sched_kwargs['request_spec']
            instance_uuids = request_spec['instance_uuids']
            LOG.exception(_("Error scheduling instances %(instance_uuids)s"),
                    locals())
            ctxt = message.ctxt
            for instance_uuid in instance_uuids:
                self.msg_runner.instance_update_at_top(ctxt,
                            {'uuid': instance_uuid,
                             'vm_state': vm_states.ERROR})
                try:
                    self.db.instance_update(ctxt,
                                            instance_uuid,
                                            {'vm_state': vm_states.ERROR})
                except Exception:
                    pass
