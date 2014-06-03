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

import collections

from oslo_config import cfg
from oslo_serialization import jsonutils
from oslo_utils import timeutils

from nova.cells import opts as cell_opts
from nova import db
from nova import objects
from nova.openstack.common import memorycache
from nova import utils

# NOTE(vish): azs don't change that often, so cache them for an hour to
#             avoid hitting the db multiple times on every request.
AZ_CACHE_SECONDS = 60 * 60
MC = None

availability_zone_opts = [
    cfg.StrOpt('internal_service_availability_zone',
               default='internal',
               help='The availability_zone to show internal services under'),
    cfg.StrOpt('default_availability_zone',
               default='nova',
               help='Default compute node availability_zone'),
    ]

CONF = cfg.CONF
CONF.import_opt('mute_child_interval', 'nova.cells.opts', group='cells')
CONF.register_opts(availability_zone_opts)


def _get_cache():
    global MC

    if MC is None:
        MC = memorycache.get_client()

    return MC


def reset_cache():
    """Reset the cache, mainly for testing purposes and update
    availability_zone for host aggregate
    """

    global MC

    MC = None


def _make_cache_key(host, cell_name=None):
    return "azcache-%s-%s" % (cell_name or 'none', host.encode('utf-8'))


def _build_metadata_by_host(aggregates, hosts=None):
    if hosts and not isinstance(hosts, set):
        hosts = set(hosts)
    metadata = collections.defaultdict(set)
    for aggregate in aggregates:
        for host in aggregate.hosts:
            if hosts and host not in hosts:
                continue
            metadata[host].add(aggregate.metadata.values()[0])
    return metadata


def set_availability_zones(context, services):
    # Makes sure services isn't a sqlalchemy object
    services = [dict(service.iteritems()) for service in services]
    hosts = set([service['host'] for service in services])
    aggregates = objects.AggregateList.get_by_metadata_key(context,
            'availability_zone', hosts=hosts)
    metadata = _build_metadata_by_host(aggregates, hosts=hosts)
    # gather all of the availability zones associated with a service host
    for service in services:
        az = CONF.internal_service_availability_zone
        if service['topic'] == "compute":
            if metadata.get(service['host']):
                az = u','.join(list(metadata[service['host']]))
            else:
                az = CONF.default_availability_zone
                # update the cache
                update_host_availability_zone_cache(context,
                                                    service['host'], az)
        service['availability_zone'] = az
    return services


def get_host_availability_zone(context, host):
    aggregates = objects.AggregateList.get_by_host(context, host,
                                                   key='availability_zone')
    if aggregates:
        az = aggregates[0].metadata['availability_zone']
    else:
        az = CONF.default_availability_zone
    return az


def update_host_availability_zone_cache(context, host, availability_zone=None):
    if not availability_zone:
        availability_zone = get_host_availability_zone(context, host)
    cache = _get_cache()
    cache_key = _make_cache_key(host)
    cache.delete(cache_key)
    cache.set(cache_key, availability_zone, AZ_CACHE_SECONDS)


def get_availability_zones(context, get_only_available=False,
                           with_hosts=False):
    """Return available and unavailable zones on demand.

        :param get_only_available: flag to determine whether to return
            available zones only, default False indicates return both
            available zones and not available zones, True indicates return
            available zones only
        :param with_hosts: whether to return hosts part of the AZs
        :type with_hosts: bool
    """
    # Override for cells
    cell_type = cell_opts.get_cell_type()
    if cell_type == 'api':
        ctxt = context.elevated()
        global_azs = []
        mute_azs = []
        secs = CONF.cells.mute_child_interval
        for cell in db.cell_get_all(ctxt):
            last_seen = cell.updated_at
            capabilities = jsonutils.loads(cell.capabilities)
            if 'availability_zones' not in capabilities:
                continue
            if last_seen and timeutils.is_older_than(last_seen, secs):
                mute_azs.extend(capabilities['availability_zones'])
            else:
                global_azs.extend(capabilities['availability_zones'])
            available_zones = list(set(global_azs))
            unavailable_zones = list(set(mute_azs))
        if get_only_available:
            return available_zones
        return (available_zones, unavailable_zones)

    enabled_services = objects.ServiceList.get_all(context, disabled=False,
                                                   set_zones=False)
    enabled_services = set_availability_zones(context, enabled_services)

    available_zones = []
    for (zone, host) in [(service['availability_zone'], service['host'])
                         for service in enabled_services]:
        if not with_hosts and zone not in available_zones:
            available_zones.append(zone)
        elif with_hosts:
            _available_zones = dict(available_zones)
            zone_hosts = _available_zones.setdefault(zone, set())
            zone_hosts.add(host)
            # .items() returns a view in Py3, casting it to list for Py2 compat
            available_zones = list(_available_zones.items())

    if not get_only_available:
        disabled_services = objects.ServiceList.get_all(context, disabled=True,
                                                        set_zones=True)
        not_available_zones = []
        azs = available_zones if not with_hosts else dict(available_zones)
        zones = [(service['availability_zone'], service['host'])
                 for service in disabled_services
                 if service['availability_zone'] not in azs]
        for (zone, host) in zones:
            if not with_hosts and zone not in not_available_zones:
                not_available_zones.append(zone)
            elif with_hosts:
                _not_available_zones = dict(not_available_zones)
                zone_hosts = _not_available_zones.setdefault(zone, set())
                zone_hosts.add(host)
                # .items() returns a view in Py3, casting it to list for Py2
                #   compat
                not_available_zones = list(_not_available_zones.items())
        return (available_zones, not_available_zones)
    else:
        return available_zones


def get_instance_availability_zone(context, instance):
    """Return availability zone of specified instance."""
    host = str(instance.get('host'))
    if not host:
        return None
    cache_string = host
    cell_type = cell_opts.get_cell_type()
    if cell_type == 'api':
        cell_name = str(instance.get('cell_name'))
        if not cell_name:
            return None
        cache_string += cell_name
    cache_key = _make_cache_key(cache_string)
    cache = _get_cache()
    az = cache.get(cache_key)
    if not az:
        elevated = context.elevated()
        if cell_type == 'api':
            sys_metadata = utils.instance_sys_meta(instance)
            az = sys_metadata.get('availability_zone')
            if not az:
                return None
        else:
            az = get_host_availability_zone(elevated, host)
        cache.set(cache_key, az, AZ_CACHE_SECONDS)
    return az
