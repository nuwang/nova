# Copyright (c) 2012 OpenStack Foundation
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

"""Availability zone helper functions."""

from oslo.config import cfg

from nova.cells import rpcapi as cell_rpcapi
from nova import db
from nova.openstack.common import log as logging
from nova.openstack.common import memorycache
from nova.openstack.common import timeutils

AZ_CACHE_SECONDS = 60 * 60 * 24
MC = memorycache.get_client()

availability_zone_opts = [
    cfg.StrOpt('internal_service_availability_zone',
               default='internal',
               help='availability_zone to show internal services under'),
    cfg.StrOpt('default_availability_zone',
               # deprecated in Grizzly release
               deprecated_name='node_availability_zone',
               default='nova',
               help='default compute node availability_zone'),
    ]

CONF = cfg.CONF
CONF.import_opt('mute_child_interval', 'nova.cells.opts', group='cells')
CONF.register_opts(availability_zone_opts)

LOG = logging.getLogger(__name__)


def set_availability_zones(context, services):
    # Makes sure services isn't a sqlalchemy object
    services = [dict(service.iteritems()) for service in services]
    metadata = db.aggregate_host_get_by_metadata_key(context,
            key='availability_zone')
    for service in services:
        az = CONF.internal_service_availability_zone
        if service['topic'] == "compute":
            if metadata.get(service['host']):
                az = u','.join(list(metadata[service['host']]))
            else:
                az = CONF.default_availability_zone
        service['availability_zone'] = az
    return services


def get_host_availability_zone(context, host, conductor_api=None, cell=None):
    if cell and CONF.cells.enable:

        cells_rpcapi = cell_rpcapi.CellsAPI()
        az = cells_rpcapi.get_host_availability_zone(context, cell, host)
        return az
    if conductor_api:
        metadata = conductor_api.aggregate_metadata_get_by_host(
            context, host, key='availability_zone')
    else:
        metadata = db.aggregate_metadata_get_by_host(
            context, host, key='availability_zone')
    if 'availability_zone' in metadata:
        return list(metadata['availability_zone'])[0]
    else:
        return CONF.default_availability_zone


def get_availability_zones(context, cells_api=False):
    """Return available and unavailable zones."""
    # Override for cells
    if cells_api:
        cells_rpcapi = cell_rpcapi.CellsAPI()
        cell_info = cells_rpcapi.get_cell_info_for_neighbors(context)
        global_azs = []
        mute_azs = []
        secs = CONF.cells.mute_child_interval
        for cell in cell_info:
            last_seen = cell['last_seen']
            if 'availability_zones' not in cell['capabilities']:
                continue
            if last_seen and timeutils.is_older_than(last_seen, secs):
                mute_azs.extend(cell['capabilities']['availability_zones'])
            else:
                global_azs.extend(cell['capabilities']['availability_zones'])
        available_zones = list(set(global_azs))
        unavailable_zones = list(set(mute_azs))
        return (available_zones, unavailable_zones)

    enabled_services = db.service_get_all(context, False)
    disabled_services = db.service_get_all(context, True)
    enabled_services = set_availability_zones(context, enabled_services)
    disabled_services = set_availability_zones(context, disabled_services)

    available_zones = []
    for zone in [service['availability_zone'] for service
                 in enabled_services]:
        if zone not in available_zones:
            available_zones.append(zone)

    not_available_zones = []
    zones = [service['availability_zone'] for service in disabled_services
            if service['availability_zone'] not in available_zones]
    for zone in zones:
        if zone not in not_available_zones:
            not_available_zones.append(zone)
    return (available_zones, not_available_zones)


def get_instance_availability_zone(context, instance):
    """Return availability zone of specified instance."""
    host = str(instance.get('host'))
    if not host:
        return None
    cell_name = str(instance.get('cell_name', None))

    if cell_name == 'None':
        cell_name = None

    cache_key = "azcache-%s-%s" % (cell_name or 'none', host)
    az = MC.get(cache_key)
    if not az:
        elevated = context.elevated()
        az = get_host_availability_zone(elevated, host, cell=cell_name)
        MC.set(cache_key, az, AZ_CACHE_SECONDS)
    return az
