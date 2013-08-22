from nova import db
from nova.openstack.common import log as logging
from nova.scheduler import filters


LOG = logging.getLogger(__name__)


class AggregateInstanceTypesFilter(filters.BaseHostFilter):
    """AggregateInstanceTypesFilter works with InstanceType records."""

    def host_passes(self, host_state, filter_properties):
        """Return a list of hosts that can create instance_type
        Checks that the instance_type name matches the instance_type
        metadata provided by the aggregates.  If not present return False.
        """
        instance_type = filter_properties.get('instance_type')

        context = filter_properties['context'].elevated()
        metadata = db.aggregate_metadata_get_by_host(
            context, host_state.host, key='instance_type')
        host_instance_types = metadata.get('instance_type', None)

        if host_instance_types:
             host_instance_types = list(metadata['instance_type'])
             if instance_type['name'] in host_instance_types:
                 return True

        LOG.debug(_("%(host_state)s fails instance_type name "
                    "requirements"), locals())
        return False
