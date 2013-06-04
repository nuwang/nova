from oslo.config import cfg

from nova.cells import filters
from nova.openstack.common import log as logging
from nova.availability_zones import get_availability_zones

LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.import_opt('default_availability_zone', 'nova.availability_zones')


class AvailabilityZoneFilter(filters.BaseCellFilter):
    """Filters cells by availability zone.

    Works with cell capabilities using the key
    'availability_zones'
    Note: cell can have multiple availability zones
    """

    def cell_passes(self, cell, filter_properties):
        LOG.debug('Filtering on availability zones for cell %s' % cell)
        
        spec = filter_properties.get('request_spec', {})
        props = spec.get('instance_properties', {})
        availability_zone = props.get('availability_zone')

        if availability_zone:
            available_zones = cell.capabilities.get('availability_zones', [])
            return availability_zone in available_zones

        return True

 
