from oslo_log import log as logging

from nova.cells import filters

LOG = logging.getLogger(__name__)


class AvailabilityZoneFilter(filters.BaseCellFilter):
    """Filters cells by availability zone.

    Works with cell capabilities using the key
    'availability_zones'
    Note: cell can have multiple availability zones
    """

    def cell_passes(self, cell, filter_properties):
        LOG.debug('Filtering on availability zones for cell %s' % cell)

        available_zones = cell.capabilities.get('availability_zones', [])
        LOG.debug('Available zones: %s' % available_zones)
        spec = filter_properties.get('request_spec', {})
        props = spec.get('instance_properties', {})
        availability_zone = props.get('availability_zone')

        if availability_zone:
            return availability_zone in available_zones

        return True
